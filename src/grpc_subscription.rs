//! gRPC Subscription Manager for Yellowstone Geyser
//!
//! Provides an alternative to WebSocket-based subscriptions using
//! Yellowstone gRPC (Geyser plugin) for lower-latency account updates.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use log::{debug, error, info, warn};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel as SolanaCommitmentLevel};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::{sleep, MissedTickBehavior};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::{GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestPing,
};

use crate::subscription::{AccountUpdate, CachedAccount};

const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
const RESUBSCRIBE_DEBOUNCE_MS: u64 = 200;
const RECONNECT_INITIAL_SECS: u64 = 1;
const RECONNECT_MAX_SECS: u64 = 30;
const DEFAULT_ACCOUNTS_PER_FILTER: usize = 100;
const GRPC_SEND_TIMEOUT_SECS: u64 = 5;

enum SubscriptionCommand {
    Subscribe(Pubkey),
    Unsubscribe(Pubkey),
}

struct SubscriptionState {
    cached: Option<CachedAccount>,
    ref_count: usize,
}

pub struct GrpcSubscriptionManager {
    subscriptions: Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
    update_tx: broadcast::Sender<AccountUpdate>,
    heartbeat: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    command_tx: mpsc::UnboundedSender<SubscriptionCommand>,
    _task_handle: tokio::task::JoinHandle<()>,
}

impl GrpcSubscriptionManager {
    pub async fn new(
        endpoint: String,
        x_token: Option<String>,
        commitment: CommitmentConfig,
    ) -> Result<Self> {
        let subscriptions = Arc::new(DashMap::new());
        let (update_tx, _) = broadcast::channel(1024);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let heartbeat = Arc::new(AtomicU64::new(now_secs()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let commitment_level = map_commitment(commitment);
        let x_token = x_token.filter(|token| !token.trim().is_empty());

        let endpoint_clone = endpoint.clone();
        let subscriptions_clone = Arc::clone(&subscriptions);
        let update_tx_clone = update_tx.clone();
        let heartbeat_clone = Arc::clone(&heartbeat);
        let shutdown_clone = Arc::clone(&shutdown);

        let task_handle = tokio::spawn(async move {
            if let Err(err) = Self::run_loop(
                endpoint_clone,
                x_token,
                commitment_level,
                subscriptions_clone,
                update_tx_clone,
                heartbeat_clone,
                shutdown_clone,
                command_rx,
            )
            .await
            {
                error!("[GRPC] Subscription loop exited: {err:?}");
            }
        });

        info!("[GRPC] GrpcSubscriptionManager initialized");

        Ok(Self {
            subscriptions,
            update_tx,
            heartbeat,
            shutdown,
            command_tx,
            _task_handle: task_handle,
        })
    }

    pub fn subscribe_updates(&self) -> broadcast::Receiver<AccountUpdate> {
        self.update_tx.subscribe()
    }

    pub fn is_healthy(&self) -> bool {
        let now_secs = now_secs();
        let elapsed = now_secs.saturating_sub(self.heartbeat.load(Ordering::SeqCst));
        elapsed <= HEARTBEAT_TIMEOUT_SECS
    }

    pub async fn ensure_subscription(&self, account: Pubkey) -> Result<()> {
        let entry = self.subscriptions.entry(account).or_insert_with(|| {
            Arc::new(RwLock::new(SubscriptionState {
                cached: None,
                ref_count: 0,
            }))
        });
        let state_lock = Arc::clone(entry.value());
        drop(entry);

        let mut state = state_lock.write().await;
        if state.ref_count == 0 {
            state.ref_count = 1;
            if self
                .command_tx
                .send(SubscriptionCommand::Subscribe(account))
                .is_err()
            {
                state.ref_count = 0;
                return Err(anyhow::anyhow!("gRPC subscription worker unavailable"));
            }
        } else {
            state.ref_count += 1;
        }
        Ok(())
    }

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
            self.subscriptions.remove(account);
            if self
                .command_tx
                .send(SubscriptionCommand::Unsubscribe(*account))
                .is_err()
            {
                warn!("[GRPC] Failed to send unsubscribe command for {}", account);
            }
        }
    }

    pub async fn get_cached(&self, account: &Pubkey, min_slot: u64) -> Option<CachedAccount> {
        let state_lock = self.subscriptions.get(account).map(|entry| Arc::clone(entry.value()))?;
        let state = state_lock.read().await;
        let cached = state.cached.as_ref()?;
        if cached.slot >= min_slot {
            Some(cached.clone())
        } else {
            None
        }
    }

    pub fn get_cached_sync(&self, account: &Pubkey) -> Option<CachedAccount> {
        let state_lock = self.subscriptions.get(account)?;
        let state_lock = Arc::clone(state_lock.value());
        let state = state_lock.try_read().ok()?;
        state.cached.clone()
    }

    pub async fn prefill_cache(&self, account: &Pubkey, data: Vec<u8>, slot: u64) {
        let entry = self.subscriptions.entry(*account).or_insert_with(|| {
            Arc::new(RwLock::new(SubscriptionState {
                cached: None,
                ref_count: 0,
            }))
        });
        let state_lock = Arc::clone(entry.value());
        drop(entry);

        let mut state = state_lock.write().await;
        let should_update = match &state.cached {
            None => true,
            Some(cached) => slot > cached.slot,
        };

        if should_update {
            state.cached = Some(CachedAccount::new(data, slot));
        }
    }

    pub fn stats(&self) -> GrpcSubscriptionStats {
        GrpcSubscriptionStats {
            total_subscriptions: self.subscriptions.len(),
        }
    }

    pub async fn shutdown(&self) {
        info!("[GRPC] Shutting down GrpcSubscriptionManager...");
        self.shutdown.store(true, Ordering::SeqCst);
        self.subscriptions.clear();
        info!("[GRPC] GrpcSubscriptionManager shutdown complete");
    }

    async fn run_loop(
        endpoint: String,
        x_token: Option<String>,
        commitment: CommitmentLevel,
        subscriptions: Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
        update_tx: broadcast::Sender<AccountUpdate>,
        heartbeat: Arc<AtomicU64>,
        shutdown: Arc<AtomicBool>,
        mut command_rx: mpsc::UnboundedReceiver<SubscriptionCommand>,
    ) -> Result<()> {
        let mut backoff = Duration::from_secs(RECONNECT_INITIAL_SECS);

        loop {
            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            match Self::connect_client(&endpoint, x_token.clone()).await {
                Ok(client) => {
                    info!("[GRPC] ✅ Connected to gRPC endpoint successfully");
                    backoff = Duration::from_secs(RECONNECT_INITIAL_SECS);
                    if let Err(err) = Self::run_stream(
                        client,
                        commitment,
                        &subscriptions,
                        &update_tx,
                        &heartbeat,
                        &shutdown,
                        &mut command_rx,
                    )
                    .await
                    {
                        warn!("[GRPC] Stream exited: {err:?}");
                    }
                }
                Err(err) => {
                    warn!("[GRPC] Failed to connect: {err:?}");
                }
            }

            if shutdown.load(Ordering::SeqCst) {
                break;
            }

            warn!("[GRPC] Reconnecting in {:?}...", backoff);
            sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(Duration::from_secs(RECONNECT_MAX_SECS));
        }

        Ok(())
    }

    async fn connect_client(
        endpoint: &str,
        x_token: Option<String>,
    ) -> Result<GeyserGrpcClient<impl Interceptor>> {
        let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
            .x_token(x_token)?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10));

        if endpoint.starts_with("https://") {
            // ✅ 使用系统原生 CA 证书来验证服务器证书
            builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }

        let client = builder.connect().await?;
        Ok(client)
    }

    async fn run_stream(
        mut client: GeyserGrpcClient<impl Interceptor>,
        commitment: CommitmentLevel,
        subscriptions: &Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
        update_tx: &broadcast::Sender<AccountUpdate>,
        heartbeat: &Arc<AtomicU64>,
        shutdown: &Arc<AtomicBool>,
        command_rx: &mut mpsc::UnboundedReceiver<SubscriptionCommand>,
    ) -> Result<()> {
        let mut accounts = collect_accounts(subscriptions);
        let initial_request = build_subscribe_request(&accounts, commitment, None);
        let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(initial_request)).await?;
        
        info!("[GRPC] Stream established, subscribed to {} accounts", accounts.len());

        let mut pending_update = false;
        let mut resub_interval = tokio::time::interval(Duration::from_millis(RESUBSCRIBE_DEBOUNCE_MS));
        resub_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut heartbeat_interval =
            tokio::time::interval(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS / 2));
        heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    heartbeat.store(now_secs(), Ordering::SeqCst);
                    if shutdown.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                }
                _ = resub_interval.tick() => {
                    if pending_update {
                        let request = build_subscribe_request(&accounts, commitment, None);
                        match tokio::time::timeout(
                            Duration::from_secs(GRPC_SEND_TIMEOUT_SECS),
                            subscribe_tx.send(request),
                        )
                        .await
                        {
                            Ok(Ok(())) => {
                                pending_update = false;
                            }
                            Ok(Err(err)) => {
                                bail!("failed to send subscribe update: {err}");
                            }
                            Err(_) => {
                                bail!(
                                    "subscribe update send timed out after {}s",
                                    GRPC_SEND_TIMEOUT_SECS
                                );
                            }
                        }
                    }
                }
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(SubscriptionCommand::Subscribe(account)) => {
                            if accounts.insert(account) {
                                pending_update = true;
                            }
                        }
                        Some(SubscriptionCommand::Unsubscribe(account)) => {
                            if accounts.remove(&account) {
                                pending_update = true;
                            }
                        }
                        None => return Ok(()),
                    }
                }
                message = stream.next() => {
                    match message {
                        Some(Ok(update)) => {
                            heartbeat.store(now_secs(), Ordering::SeqCst);
                            if let Some(update_oneof) = update.update_oneof {
                                match update_oneof {
                                    UpdateOneof::Account(account_update) => {
                                        Self::process_account_update(account_update, subscriptions, update_tx).await;
                                    }
                                    UpdateOneof::Ping(_) => {
                                        // 仅发送 ping 响应，不重发完整订阅（官方示例）
                                        let pong = SubscribeRequest {
                                            accounts: HashMap::default(),
                                            slots: HashMap::default(),
                                            transactions: HashMap::default(),
                                            transactions_status: HashMap::default(),
                                            blocks: HashMap::default(),
                                            blocks_meta: HashMap::default(),
                                            entry: HashMap::default(),
                                            commitment: None,
                                            accounts_data_slice: Vec::default(),
                                            ping: Some(SubscribeRequestPing { id: 1 }),
                                            from_slot: None,
                                        };
                                        match tokio::time::timeout(
                                            Duration::from_secs(GRPC_SEND_TIMEOUT_SECS),
                                            subscribe_tx.send(pong),
                                        )
                                        .await
                                        {
                                            Ok(Ok(())) => {}
                                            Ok(Err(err)) => {
                                                bail!("failed to send ping response: {err}");
                                            }
                                            Err(_) => {
                                                bail!(
                                                    "ping response send timed out after {}s",
                                                    GRPC_SEND_TIMEOUT_SECS
                                                );
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Some(Err(err)) => {
                            bail!("stream error: {err:?}");
                        }
                        None => {
                            bail!("stream closed");
                        }
                    }
                }
            }
        }
    }

    async fn process_account_update(
        update: yellowstone_grpc_proto::prelude::SubscribeUpdateAccount,
        subscriptions: &Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
        update_tx: &broadcast::Sender<AccountUpdate>,
    ) {
        let account_info = match update.account {
            Some(info) => info,
            None => return,
        };

        let Ok(pubkey_bytes) = account_info.pubkey.as_slice().try_into() else {
            warn!("[GRPC] Invalid pubkey length: {}", account_info.pubkey.len());
            return;
        };
        let account = Pubkey::new_from_array(pubkey_bytes);
        let slot = update.slot;
        let data = account_info.data;

        let state_lock = match subscriptions.get(&account) {
            Some(entry) => Arc::clone(entry.value()),
            None => return,
        };
        let mut state = state_lock.write().await;
        let should_update = match &state.cached {
            None => true,
            Some(cached) => slot >= cached.slot,
        };
        if should_update {
            state.cached = Some(CachedAccount::new(data.clone(), slot));
            debug!("[GRPC] Updated cache for account {} at slot {}", account, slot);
            let _ = update_tx.send(AccountUpdate { account, data, slot });
        }
    }
}

fn collect_accounts(
    subscriptions: &Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
) -> HashSet<Pubkey> {
    subscriptions
        .iter()
        .filter_map(|entry| {
            let state_lock = entry.value();
            match state_lock.try_read() {
                Ok(state) if state.ref_count > 0 => Some(*entry.key()),
                Ok(_) => None,
                Err(_) => Some(*entry.key()),
            }
        })
        .collect()
}

pub struct GrpcSubscriptionStats {
    pub total_subscriptions: usize,
}

fn map_commitment(commitment: CommitmentConfig) -> CommitmentLevel {
    match commitment.commitment {
        SolanaCommitmentLevel::Processed => CommitmentLevel::Processed,
        SolanaCommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
        SolanaCommitmentLevel::Finalized => CommitmentLevel::Finalized,
    }
}

fn build_subscribe_request(
    accounts: &HashSet<Pubkey>,
    commitment: CommitmentLevel,
    ping_id: Option<i32>,
) -> SubscribeRequest {
    let account_filters = build_account_filters(accounts);
    SubscribeRequest {
        accounts: account_filters,
        slots: HashMap::default(),
        transactions: HashMap::default(),
        transactions_status: HashMap::default(),
        entry: HashMap::default(),
        blocks: HashMap::default(),
        blocks_meta: HashMap::default(),
        commitment: Some(commitment as i32),
        accounts_data_slice: Vec::default(),
        ping: ping_id.map(|id| SubscribeRequestPing { id }),
        from_slot: None,
    }
}

fn build_account_filters(accounts: &HashSet<Pubkey>) -> HashMap<String, SubscribeRequestFilterAccounts> {
    if accounts.is_empty() {
        return HashMap::new();
    }

    let mut keys: Vec<String> = accounts.iter().map(|k| k.to_string()).collect();
    keys.sort();

    let mut filters = HashMap::new();
    let chunk_size = max_accounts_per_filter();
    for (idx, chunk) in keys.chunks(chunk_size).enumerate() {
        filters.insert(
            format!("accounts-{}", idx),
            SubscribeRequestFilterAccounts {
                account: chunk.to_vec(),
                owner: Vec::new(),
                filters: Vec::new(),
                nonempty_txn_signature: None,
            },
        );
    }

    filters
}

fn max_accounts_per_filter() -> usize {
    std::env::var("GRPC_MAX_ACCOUNTS_PER_FILTER")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(DEFAULT_ACCOUNTS_PER_FILTER)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
