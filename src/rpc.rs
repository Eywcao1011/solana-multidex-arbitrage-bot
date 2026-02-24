use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use dashmap::DashMap;
use log::debug;
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    nonblocking::rpc_client::RpcClient,
    rpc_request::{RpcError, RpcResponseErrorData},
};
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;

use crate::{
    cache::SlotAwareCache,
    constants::{METEORA_LB_PROGRAM_ID, PUMP_FUN_GLOBAL_CONFIG_SEED, PUMP_FUN_PROGRAM_ID},
    dex::{meteora_lb, orca, raydium, AccountReader, PoolFees},
    subscription::SubscriptionProvider,
};


#[derive(Clone, Debug)]
pub struct MintInfo {
    pub decimals: u8,
    /// Token program that owns this mint (spl_token::ID or Token2022)
    pub owner: Pubkey,
}

#[derive(Clone, Debug)]
pub struct PumpFunFees {
    pub lp_fee_bps: u64,
    pub protocol_fee_bps: u64,
    pub coin_creator_fee_bps: u64,
    pub protocol_fee_recipient: Pubkey,
}

#[derive(Clone, Debug)]
pub struct PumpFunFeeBasis {
    pub lp_fee_bps: u64,
    pub protocol_fee_bps: u64,
    pub coin_creator_fee_bps: u64,
}

#[derive(Clone, Debug)]
pub struct PumpFunFeeTier {
    pub market_cap_lamports_threshold: u128,
    pub fees: PumpFunFeeBasis,
}

#[derive(Clone, Debug)]
pub struct PumpFunFeeConfig {
    pub flat_fees: PumpFunFeeBasis,
    pub fee_tiers: Vec<PumpFunFeeTier>,
    pub protocol_fee_recipient: Pubkey,
}

impl PumpFunFeeConfig {
    pub fn select_fees(&self, market_cap_lamports: Option<u128>) -> PumpFunFees {
        let mut selected = &self.flat_fees;
        if let Some(market_cap) = market_cap_lamports {
            for tier in &self.fee_tiers {
                if market_cap >= tier.market_cap_lamports_threshold {
                    selected = &tier.fees;
                }
            }
        }

        PumpFunFees {
            lp_fee_bps: selected.lp_fee_bps,
            protocol_fee_bps: selected.protocol_fee_bps,
            coin_creator_fee_bps: selected.coin_creator_fee_bps,
            protocol_fee_recipient: self.protocol_fee_recipient,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RaydiumFees {
    pub trade_fee_rate: u32,
    pub protocol_fee_rate: u32,
    pub fund_fee_rate: u32,
}

#[derive(Clone)]
pub struct AppContext {
    rpc_http: Arc<RpcClient>,
    // ✅ 普通缓存（mint 信息不会变）
    mint_cache: Arc<DashMap<Pubkey, MintInfo>>,
    pump_fun_fees: Arc<Mutex<Option<PumpFunFeeConfig>>>,
    raydium_config_cache: Arc<DashMap<Pubkey, RaydiumFees>>,
    account_owner_cache: Arc<DashMap<Pubkey, Pubkey>>,
    // ✅ 带 slot 验证的缓存（流动性数据会变）
    #[allow(dead_code)]
    orca_tick_cache: Arc<SlotAwareCache<(Pubkey, i32), Arc<orca::TickArrayState>>>,
    #[allow(dead_code)]
    raydium_tick_cache: Arc<SlotAwareCache<(Pubkey, i32), Arc<raydium::TickArrayState>>>,
    #[allow(dead_code)]
    meteora_bin_array_cache: Arc<SlotAwareCache<(Pubkey, i64), Arc<meteora_lb::BinArrayState>>>,
    #[allow(dead_code)]
    meteora_bitmap_cache: Arc<SlotAwareCache<Pubkey, Option<Arc<meteora_lb::BitmapExtension>>>>,
    // ✅ 订阅提供器（可选，支持 WS/gRPC，启动后设置）
    subscription_provider: Arc<RwLock<Option<Arc<SubscriptionProvider>>>>,
    // ✅ Spot-only 模式：只维护价格，不订阅 tick/bin array
    spot_only_mode: bool,
    // ✅ DLMM bin arrays 缓存（从 SDK introspect 获取）
    dlmm_bin_arrays_cache: Arc<DashMap<Pubkey, Vec<Pubkey>>>,  // deprecated, merged
    // ✅ DLMM 方向性 bin arrays 缓存
    dlmm_bin_arrays_x_to_y: Arc<DashMap<Pubkey, Vec<Pubkey>>>,  // Sell direction (X→Y)
    dlmm_bin_arrays_y_to_x: Arc<DashMap<Pubkey, Vec<Pubkey>>>,  // Buy direction (Y→X)
    // ✅ Orca tick arrays 缓存（类似 DLMM，从 warmup 获取）
    orca_tick_arrays_cache: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
    // ✅ Pool fee overrides（用于缓存并固定池子手续费）
    pool_fee_overrides: Arc<DashMap<Pubkey, PoolFees>>,
    // ✅ Blockhash 缓存（15 秒刷新，避免每笔交易都查询）
    blockhash_cache: Arc<RwLock<Option<(Hash, Instant)>>>,
    // ✅ Jito tip 账户缓存（周期性刷新）
    jito_tip_account: Arc<RwLock<Option<Pubkey>>>,
}

impl AppContext {
    pub fn new(http_url: String, commitment: CommitmentConfig) -> Self {
        use std::time::Duration;

        let rpc_http = Arc::new(RpcClient::new_with_commitment(http_url, commitment));

        Self {
            rpc_http,
            mint_cache: Arc::new(DashMap::new()),
            pump_fun_fees: Arc::new(Mutex::new(None)),
            raydium_config_cache: Arc::new(DashMap::new()),
            account_owner_cache: Arc::new(DashMap::new()),
            // ✅ Tick/bin array 缓存：使用更短的缓存窗口以减少陈旧数据风险
            // ⚠️ 这些账户在每次 swap 时都会变化，所以使用更短的缓存时间
            orca_tick_cache: Arc::new(SlotAwareCache::new(2, Duration::from_secs(10))),
            raydium_tick_cache: Arc::new(SlotAwareCache::new(2, Duration::from_secs(10))),
            meteora_bin_array_cache: Arc::new(SlotAwareCache::new(2, Duration::from_secs(10))),
            meteora_bitmap_cache: Arc::new(SlotAwareCache::new(2, Duration::from_secs(10))),
            subscription_provider: Arc::new(RwLock::new(None)),
            spot_only_mode: true,
            dlmm_bin_arrays_cache: Arc::new(DashMap::new()),
            dlmm_bin_arrays_x_to_y: Arc::new(DashMap::new()),
            dlmm_bin_arrays_y_to_x: Arc::new(DashMap::new()),
            orca_tick_arrays_cache: Arc::new(DashMap::new()),
            pool_fee_overrides: Arc::new(DashMap::new()),
            blockhash_cache: Arc::new(RwLock::new(None)),
            jito_tip_account: Arc::new(RwLock::new(None)),
        }
    }

    /// 设置订阅提供器（在监控启动后调用）
    pub async fn set_subscription_manager(&self, provider: Arc<SubscriptionProvider>) {
        *self.subscription_provider.write().await = Some(provider);
    }

    pub fn rpc_client(&self) -> Arc<RpcClient> {
        Arc::clone(&self.rpc_http)
    }

    /// Spot-only 模式：只维护价格，不订阅 tick/bin array
    pub fn spot_only_mode(&self) -> bool {
        self.spot_only_mode
    }
    
    /// 创建一个用于 ALT 收集的副本
    /// 保持 spot_only_mode = true 以避免触发 tick array prefetch
    /// ALT 收集只需要静态账户，不需要精确模拟
    pub fn clone_for_alt_collection(&self) -> Self {
        Self {
            rpc_http: Arc::clone(&self.rpc_http),
            mint_cache: Arc::clone(&self.mint_cache),
            pump_fun_fees: Arc::clone(&self.pump_fun_fees),
            raydium_config_cache: Arc::clone(&self.raydium_config_cache),
            account_owner_cache: Arc::clone(&self.account_owner_cache),
            orca_tick_cache: Arc::clone(&self.orca_tick_cache),
            raydium_tick_cache: Arc::clone(&self.raydium_tick_cache),
            meteora_bin_array_cache: Arc::clone(&self.meteora_bin_array_cache),
            meteora_bitmap_cache: Arc::clone(&self.meteora_bitmap_cache),
            subscription_provider: Arc::clone(&self.subscription_provider),
            // ✅ 保持 spot_only_mode = true，避免 prefetch tick arrays
            // ALT 收集只需要静态账户信息，不需要精确模拟数据
            spot_only_mode: true,
            dlmm_bin_arrays_cache: Arc::clone(&self.dlmm_bin_arrays_cache),
            dlmm_bin_arrays_x_to_y: Arc::clone(&self.dlmm_bin_arrays_x_to_y),
            dlmm_bin_arrays_y_to_x: Arc::clone(&self.dlmm_bin_arrays_y_to_x),
            orca_tick_arrays_cache: Arc::clone(&self.orca_tick_arrays_cache),
            pool_fee_overrides: Arc::clone(&self.pool_fee_overrides),
            blockhash_cache: Arc::clone(&self.blockhash_cache),
            jito_tip_account: Arc::clone(&self.jito_tip_account),
        }
    }
    
    /// 创建一个非 spot_only 的副本
    /// 用于 Pump.fun 等需要通过 RPC 获取 vault 数据的 DEX
    pub fn clone_non_spot_only(&self) -> Self {
        Self {
            rpc_http: Arc::clone(&self.rpc_http),
            mint_cache: Arc::clone(&self.mint_cache),
            pump_fun_fees: Arc::clone(&self.pump_fun_fees),
            raydium_config_cache: Arc::clone(&self.raydium_config_cache),
            account_owner_cache: Arc::clone(&self.account_owner_cache),
            orca_tick_cache: Arc::clone(&self.orca_tick_cache),
            raydium_tick_cache: Arc::clone(&self.raydium_tick_cache),
            meteora_bin_array_cache: Arc::clone(&self.meteora_bin_array_cache),
            meteora_bitmap_cache: Arc::clone(&self.meteora_bitmap_cache),
            subscription_provider: Arc::clone(&self.subscription_provider),
            // ✅ 设置 spot_only_mode = false，允许通过 RPC 获取数据
            spot_only_mode: false,
            dlmm_bin_arrays_cache: Arc::clone(&self.dlmm_bin_arrays_cache),
            dlmm_bin_arrays_x_to_y: Arc::clone(&self.dlmm_bin_arrays_x_to_y),
            dlmm_bin_arrays_y_to_x: Arc::clone(&self.dlmm_bin_arrays_y_to_x),
            orca_tick_arrays_cache: Arc::clone(&self.orca_tick_arrays_cache),
            pool_fee_overrides: Arc::clone(&self.pool_fee_overrides),
            blockhash_cache: Arc::clone(&self.blockhash_cache),
            jito_tip_account: Arc::clone(&self.jito_tip_account),
        }
    }

    /// 设置 DLMM 池的 bin arrays（从 SDK introspect 获取）
    /// Deprecated: use set_dlmm_directional_bin_arrays instead
    pub fn set_dlmm_bin_arrays(&self, pool: &Pubkey, bin_arrays: Vec<Pubkey>) {
        self.dlmm_bin_arrays_cache.insert(*pool, bin_arrays);
    }

    /// 获取 DLMM 池的 bin arrays（从缓存）
    /// Deprecated: use get_dlmm_bin_arrays_for_direction instead
    pub fn get_dlmm_bin_arrays(&self, pool: &Pubkey) -> Option<Vec<Pubkey>> {
        self.dlmm_bin_arrays_cache.get(pool).map(|v| v.clone())
    }

    /// 设置 pool 费率覆盖（用于固定手续费）
    pub fn set_pool_fee_override(&self, pool: &Pubkey, fees: PoolFees) {
        self.pool_fee_overrides.insert(*pool, fees);
    }

    /// 获取 pool 费率覆盖
    pub fn get_pool_fee_override(&self, pool: &Pubkey) -> Option<PoolFees> {
        self.pool_fee_overrides.get(pool).map(|value| value.clone())
    }

    /// 清除 pool 费率覆盖
    pub fn clear_pool_fee_override(&self, pool: &Pubkey) {
        self.pool_fee_overrides.remove(pool);
    }

    /// 刷新 Raydium amm_config 费率缓存
    pub fn clear_raydium_amm_config_cache(&self) {
        self.raydium_config_cache.clear();
    }

    /// 清理 warmup/预取类缓存（tick/bin arrays）
    pub fn clear_warmup_caches(&self) {
        self.dlmm_bin_arrays_cache.clear();
        self.dlmm_bin_arrays_x_to_y.clear();
        self.dlmm_bin_arrays_y_to_x.clear();
        self.orca_tick_arrays_cache.clear();
        self.orca_tick_cache.clear();
        self.raydium_tick_cache.clear();
        self.meteora_bin_array_cache.clear();
        self.meteora_bitmap_cache.clear();
    }

    /// 设置 Jito tip 账户缓存
    pub async fn set_jito_tip_account(&self, account: Option<Pubkey>) {
        *self.jito_tip_account.write().await = account;
    }

    /// 获取缓存的 Jito tip 账户（同步）
    pub fn get_jito_tip_account(&self) -> Option<Pubkey> {
        let guard = self.jito_tip_account.try_read().ok()?;
        *guard
    }

    /// 设置 DLMM 池的方向性 bin arrays（从 SDK introspect 获取）
    /// x_to_y: for Sell trades, y_to_x: for Buy trades
    pub fn set_dlmm_directional_bin_arrays(
        &self,
        pool: &Pubkey,
        x_to_y: Vec<Pubkey>,
        y_to_x: Vec<Pubkey>,
    ) {
        self.dlmm_bin_arrays_x_to_y.insert(*pool, x_to_y);
        self.dlmm_bin_arrays_y_to_x.insert(*pool, y_to_x);
    }

    /// 根据交易方向获取 DLMM 池的 bin arrays
    /// sell=true: X→Y direction, sell=false: Y→X direction
    pub fn get_dlmm_bin_arrays_for_direction(&self, pool: &Pubkey, sell: bool) -> Option<Vec<Pubkey>> {
        if sell {
            self.dlmm_bin_arrays_x_to_y.get(pool).map(|v| v.clone())
        } else {
            self.dlmm_bin_arrays_y_to_x.get(pool).map(|v| v.clone())
        }
    }

    /// 设置 Orca 池的 tick arrays（从 warmup 获取）
    pub fn set_orca_tick_arrays(&self, pool: &Pubkey, tick_arrays: Vec<Pubkey>) {
        self.orca_tick_arrays_cache.insert(*pool, tick_arrays);
    }

    /// 获取 Orca 池的 tick arrays（从缓存）
    pub fn get_orca_tick_arrays(&self, pool: &Pubkey) -> Option<Vec<Pubkey>> {
        self.orca_tick_arrays_cache.get(pool).map(|v| v.clone())
    }

    /// 获取缓存的 blockhash（零网络延迟）
    /// 
    /// 用于交易构建时避免网络调用
    /// 如果缓存为空或过期（>60秒），返回 None
    pub fn get_cached_blockhash(&self) -> Option<Hash> {
        // 使用 try_read 避免阻塞
        let guard = self.blockhash_cache.try_read().ok()?;
        let (hash, timestamp) = (*guard)?;
        
        // 检查是否过期（>20秒认为过期，避免提交时 blockhash 过期）
        if timestamp.elapsed().as_secs() > 20 {
            return None;
        }
        
        Some(hash)
    }

    /// 更新缓存的 blockhash
    /// 
    /// 由后台任务定期调用（每 15 秒）
    pub async fn set_cached_blockhash(&self, hash: Hash) {
        let mut guard = self.blockhash_cache.write().await;
        *guard = Some((hash, Instant::now()));
    }

    /// 获取 blockhash（优先缓存，必要时 fallback HTTP）
    /// 
    /// 交易构建时使用此方法，大部分情况下直接返回缓存，零延迟
    pub async fn get_blockhash(&self) -> Result<Hash> {
        // 1. 尝试从缓存获取
        if let Some(hash) = self.get_cached_blockhash() {
            return Ok(hash);
        }
        
        // 2. 缓存为空或过期，从 RPC 获取
        let hash = self.rpc_http
            .get_latest_blockhash()
            .await
            .context("fetch latest blockhash")?;
        
        // 3. 更新缓存
        self.set_cached_blockhash(hash).await;
        
        Ok(hash)
    }

    /// 强制刷新 blockhash（忽略缓存）
    pub async fn get_blockhash_fresh(&self) -> Result<Hash> {
        let hash = self.rpc_http
            .get_latest_blockhash()
            .await
            .context("fetch latest blockhash")?;
        self.set_cached_blockhash(hash).await;
        Ok(hash)
    }

    /// 从缓存获取 mint info（同步，零网络调用）
    /// 
    /// 用于交易构建时获取 token program，避免异步调用
    /// 如果缓存中没有，返回 None（应在 warmup 阶段预填充）
    pub fn get_mint_info_from_cache(&self, mint: &Pubkey) -> Option<MintInfo> {
        self.mint_cache.get(mint).map(|v| v.clone())
    }

    /// 获取 mint info（无 slot 绑定，使用缓存）
    ///
    /// ⚠️ 建议使用 `get_mint_info_with_slot` 以保证 slot 一致性
    pub async fn get_mint_info(&self, mint: &Pubkey) -> Result<MintInfo> {
        if let Some(cached) = self.mint_cache.get(mint) {
            return Ok(cached.clone());
        }

        let account = self
            .rpc_http
            .get_account(mint)
            .await
            .with_context(|| format!("fetch mint {}", mint))?;

        if account.data.len() < 45 {
            bail!("mint account {} too short ({} bytes)", mint, account.data.len());
        }

        // The decimals byte lives at offset 44 for SPL token mints (COption<Pubkey> + supply).
        let decimals = account.data[44];
        let info = MintInfo { decimals, owner: account.owner };
        self.mint_cache.insert(*mint, info.clone());
        Ok(info)
    }

    /// 获取 mint info，绑定到指定 slot
    ///
    /// 如果指定了 min_slot，会要求 RPC 返回至少来自该 slot 的数据
    /// 这保证了快照内所有数据来自一致的区块状态
    pub async fn get_mint_info_with_slot(
        &self,
        mint: &Pubkey,
        min_slot: Option<u64>,
    ) -> Result<MintInfo> {
        // Mint 数据不会变化，可以使用缓存
        if let Some(cached) = self.mint_cache.get(mint) {
            return Ok(cached.clone());
        }

        let account = if let Some(slot) = min_slot {
            // ✅ 使用 min_context_slot 绑定
            use solana_account_decoder::UiAccountEncoding;
            use solana_client::rpc_config::RpcAccountInfoConfig;

            let config = RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(self.rpc_http.commitment()),
                min_context_slot: Some(slot),
                ..Default::default()
            };

            match self.rpc_http.get_ui_account_with_config(mint, config).await {
                Ok(resp) => {
                    if let Some(value) = resp.value {
                        value.decode::<solana_account::Account>().ok_or_else(|| {
                            anyhow!("Failed to decode mint {} from ui account", mint)
                        })?
                    } else {
                        self.rpc_http
                            .get_account(mint)
                            .await
                            .with_context(|| format!("fetch mint {}", mint))?
                    }
                }
                Err(err) => {
                    if !is_account_missing(&err) && !is_min_context_slot_not_reached(&err) {
                        debug!(
                            "fetch mint {} with min_context_slot {} failed ({}); falling back to non-slot-bound fetch",
                            mint,
                            slot,
                            err
                        );
                    }

                    self.rpc_http
                        .get_account(mint)
                        .await
                        .with_context(|| format!("fetch mint {}", mint))?
                }
            }
        } else {
            self.rpc_http
                .get_account(mint)
                .await
                .with_context(|| format!("fetch mint {}", mint))?
        };

        if account.data.len() < 45 {
            bail!("mint account {} too short ({} bytes)", mint, account.data.len());
        }

        let decimals = account.data[44];
        let info = MintInfo { decimals, owner: account.owner };
        self.mint_cache.insert(*mint, info.clone());
        Ok(info)
    }

    /// 获取 token account 余额（无 slot 绑定）
    ///
    /// ⚠️ 建议使用 `get_token_account_amount_with_slot` 以保证 slot 一致性
    pub async fn get_token_account_amount(&self, account: &Pubkey) -> Result<u64> {
        let data = self
            .rpc_http
            .get_account_data(account)
            .await
            .with_context(|| format!("fetch token account {}", account))?;

        if data.len() < 72 {
            bail!(
                "token account {} too short for amount field ({} bytes)",
                account,
                data.len()
            );
        }

        let amount_bytes: [u8; 8] = data[64..72].try_into().expect("slice with exact length");
        Ok(u64::from_le_bytes(amount_bytes))
    }

    /// 获取 token account 余额，绑定到指定 slot
    pub async fn get_token_account_amount_with_slot(
        &self,
        account: &Pubkey,
        min_slot: Option<u64>,
    ) -> Result<u64> {
        let data = self
            .get_token_account_data_with_slot(account, min_slot)
            .await?;
        let amount_bytes: [u8; 8] = data[64..72].try_into().expect("slice with exact length");
        Ok(u64::from_le_bytes(amount_bytes))
    }

    /// 从 WS 订阅缓存获取 token account 余额（不触发 HTTP）
    /// 
    /// 用于 spot_only 模式下的零延迟价格维护
    /// 如果账户不在缓存中，返回 None
    pub fn get_token_account_amount_from_cache(&self, account: &Pubkey) -> Option<u64> {
        // 同步读取缓存 - 使用 try_read 避免阻塞
        let guard = self.subscription_provider.try_read().ok()?;
        let provider = guard.as_ref()?;
        
        // 使用 slot=0 获取任意缓存数据（不要求 slot 一致性）
        let cached = provider.get_cached_sync(account)?;
        
        if cached.data.len() < 72 {
            return None;
        }
        
        let amount_bytes: [u8; 8] = cached.data[64..72].try_into().ok()?;
        Some(u64::from_le_bytes(amount_bytes))
    }

    /// 从 WS 订阅缓存获取账户原始数据（不触发 HTTP）
    /// 
    /// 用于 spot_only 模式下解析复杂账户结构（如 Meteora Vault）
    /// 如果账户不在缓存中，返回 None
    pub fn get_account_data_from_cache(&self, account: &Pubkey) -> Option<Vec<u8>> {
        // 同步读取缓存 - 使用 try_read 避免阻塞
        let guard = self.subscription_provider.try_read().ok()?;
        let provider = guard.as_ref()?;
        
        // 获取缓存数据
        let cached = provider.get_cached_sync(account)?;
        
        Some(cached.data.clone())
    }

    /// 从 WS 订阅缓存获取 mint 总供应量（不触发 HTTP）
    /// 
    /// 用于 DAMM V1 计算池子在共享 Vault 中的份额
    /// SPL Token Mint 布局：supply 在 offset 36-44 (u64)
    pub fn get_mint_supply_from_cache(&self, mint: &Pubkey) -> Option<u64> {
        let guard = self.subscription_provider.try_read().ok()?;
        let provider = guard.as_ref()?;
        
        let cached = provider.get_cached_sync(mint)?;
        
        // Mint 账户至少需要 44 bytes（到 supply 结束）
        if cached.data.len() < 44 {
            return None;
        }
        
        // supply 在 offset 36-44
        let supply_bytes: [u8; 8] = cached.data[36..44].try_into().ok()?;
        Some(u64::from_le_bytes(supply_bytes))
    }

    /// 预填充 WS 缓存（用于 HTTP 获取后保存到缓存）
    /// 
    /// 解决问题：WS 订阅建立后，必须等账户更新才会有缓存
    /// 但某些账户（如 Vault）可能很少更新，导致每次都要 HTTP
    /// 通过 prefill_cache 保存 HTTP 获取的数据，避免重复调用
    pub async fn prefill_cache(&self, account: &Pubkey, data: Vec<u8>, slot: u64) {
        if let Some(provider) = self.subscription_provider.read().await.as_ref() {
            provider.prefill_cache(account, data, slot).await;
        }
    }

    /// 获取 mint 总供应量（通过 HTTP RPC）
    pub async fn get_mint_supply_with_slot(&self, mint: &Pubkey, min_slot: Option<u64>) -> Result<u64> {
        let data = self.fetch_account_data_with_slot(mint, min_slot).await?;
        
        if data.len() < 44 {
            bail!("mint account {} too short for supply field ({} bytes)", mint, data.len());
        }
        
        let supply_bytes: [u8; 8] = data[36..44].try_into()
            .map_err(|_| anyhow!("Failed to parse supply from mint {}", mint))?;
        Ok(u64::from_le_bytes(supply_bytes))
    }

    pub async fn get_token_account_data_with_slot(
        &self,
        account: &Pubkey,
        min_slot: Option<u64>,
    ) -> Result<Vec<u8>> {
        let data = self
            .fetch_account_data_with_slot(account, min_slot)
            .await
            .with_context(|| format!("fetch token account {}", account))?;

        if data.len() < 72 {
            bail!(
                "token account {} too short for amount field ({} bytes)",
                account,
                data.len()
            );
        }

        Ok(data)
    }

    /// 获取账户数据（优先缓存，fallback HTTP）
    pub async fn fetch_account_data_with_slot(
        &self,
        account: &Pubkey,
        min_slot: Option<u64>,
    ) -> Result<Vec<u8>> {
        if let Some(slot) = min_slot {
            if let Some(provider) = self.subscription_provider.read().await.as_ref() {
                if let Some(cached) = provider.get_cached(account, slot).await {
                    return Ok(cached.data.clone());
                }
            }
        }

        if let Some(slot) = min_slot {
            use solana_account_decoder::UiAccountEncoding;
            use solana_client::rpc_config::RpcAccountInfoConfig;

            let config = RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(self.rpc_http.commitment()),
                min_context_slot: Some(slot),
                ..Default::default()
            };

            match self.rpc_http.get_ui_account_with_config(account, config).await {
                Ok(acc) => {
                    if let Some(value) = acc.value {
                        value.data.decode().ok_or_else(|| {
                            anyhow!("Failed to decode account data for {}", account)
                        })
                    } else {
                        self.rpc_http
                            .get_account_data(account)
                            .await
                            .with_context(|| format!("fetch account {}", account))
                    }
                }
                Err(err) => {
                    if is_account_missing(&err) || is_min_context_slot_not_reached(&err) {
                        self.rpc_http
                            .get_account_data(account)
                            .await
                            .with_context(|| format!("fetch account {}", account))
                    } else {
                        Err(err.into())
                    }
                }
            }
        } else {
            self.rpc_http
                .get_account_data(account)
                .await
                .with_context(|| format!("fetch account {}", account))
        }
    }

    /// Resilient Raydium config fetcher for legacy pools.
    ///
    /// Gets Raydium AMM config, caching on success and reusing cached values
    /// on subsequent failures. Returns default zero fees only as last resort
    /// if never successfully fetched.
    /// 获取账户 owner（无 slot 绑定，使用缓存）
    ///
    /// ⚠️ 建议使用 `get_account_owner_with_slot` 以保证 slot 一致性
    ///
    /// 🛡️ 容错机制：如果 RPC 调用失败但缓存中有值，则返回缓存值
    /// 这可以防止单次 RPC 抖动导致订阅中断
    pub async fn get_account_owner(&self, account: &Pubkey) -> Result<Pubkey> {
        // 先检查缓存
        if let Some(cached) = self.account_owner_cache.get(account) {
            return Ok(*cached);
        }

        // 尝试从 RPC 获取
        match self.rpc_http.get_account(account).await {
            Ok(info) => {
                let owner = info.owner;
                self.account_owner_cache.insert(*account, owner);
                Ok(owner)
            }
            Err(err) => {
                // ✅ 容错：如果 RPC 失败，尝试返回缓存中的旧值
                // 这在 tick array 订阅中很重要，可以避免因 RPC 抖动而中断订阅
                if let Some(cached) = self.account_owner_cache.get(account) {
                    log::warn!(
                        "RPC failed to get owner for {}, using cached value: {}",
                        account,
                        err
                    );
                    return Ok(*cached);
                }

                // 如果缓存也没有，才真正失败
                anyhow::bail!(
                    "Failed to fetch account {} and no cached owner available: {}",
                    account,
                    err
                )
            }
        }
    }

    /// 获取账户 owner，绑定到指定 slot
    ///
    /// 🛡️ 容错机制：如果 RPC 调用失败但缓存中有值，则返回缓存值
    pub async fn get_account_owner_with_slot(
        &self,
        account: &Pubkey,
        min_slot: Option<u64>,
    ) -> Result<Pubkey> {
        // Owner 通常不会变化，可以使用缓存
        if let Some(cached) = self.account_owner_cache.get(account) {
            return Ok(*cached);
        }

        // 尝试从 RPC 获取 owner
        let owner_result = if let Some(slot) = min_slot {
            // ✅ 使用 min_context_slot 绑定
            use solana_account_decoder::UiAccountEncoding;
            use solana_client::rpc_config::RpcAccountInfoConfig;

            let config = RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(self.rpc_http.commitment()),
                min_context_slot: Some(slot),
                ..Default::default()
            };

            match self.rpc_http.get_ui_account_with_config(account, config).await {
                Ok(info) => {
                    if let Some(value) = info.value {
                        Pubkey::from_str(&value.owner)
                            .map_err(|err| anyhow!("Invalid owner for {}: {}", account, err))
                    } else {
                        self.rpc_http
                            .get_account(account)
                            .await
                            .map(|info| info.owner)
                            .with_context(|| format!("fetch account {}", account))
                    }
                }
                Err(err) => {
                    if is_account_missing(&err) || is_min_context_slot_not_reached(&err) {
                        self.rpc_http
                            .get_account(account)
                            .await
                            .map(|info| info.owner)
                            .with_context(|| format!("fetch account {}", account))
                    } else {
                        Err(err.into())
                    }
                }
            }
        } else {
            self.rpc_http
                .get_account(account)
                .await
                .map(|info| info.owner)
                .with_context(|| format!("fetch account {}", account))
        };

        match owner_result {
            Ok(owner) => {
                self.account_owner_cache.insert(*account, owner);
                Ok(owner)
            }
            Err(err) => {
                // ✅ 容错：如果 RPC 失败，尝试返回缓存中的旧值
                if let Some(cached) = self.account_owner_cache.get(account) {
                    log::warn!(
                        "RPC failed to get owner for {} with slot {:?}, using cached value: {}",
                        account,
                        min_slot,
                        err
                    );
                    return Ok(*cached);
                }

                // 如果缓存也没有，才真正失败
                Err(err)
            }
        }
    }

    /// 获取 Orca tick array，优先使用 WebSocket 订阅缓存
    pub async fn get_orca_tick_array(
        &self,
        program_id: &Pubkey,
        pool: &Pubkey,
        start_tick_index: i32,
        tick_spacing: i32,
        current_slot: u64,
    ) -> Result<Option<Arc<orca::TickArrayState>>> {
        let address =
            orca::tick_array_address(program_id, pool, start_tick_index).with_context(|| {
                format!("derive Orca tick array for pool {pool} start {start_tick_index}")
            })?;

        // ✅ 1. 优先从订阅缓存读取（< 1ms）
        if let Some(provider) = self.subscription_provider.read().await.as_ref() {
            if let Some(cached) = provider.get_cached(&address, current_slot).await {
                // 从缓存数据解码 tick array
                if let Ok(decoded) = orca::decode_tick_array(&cached.data, tick_spacing) {
                    return Ok(Some(Arc::new(decoded)));
                }
            }
        }

        // ✅ 2. 缓存未命中，回退 HTTP（50-200ms）
        match self.rpc_http.get_account_data(&address).await {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(None);
                }
                let decoded = orca::decode_tick_array(&data, tick_spacing)?;
                Ok(Some(Arc::new(decoded)))
            }
            Err(err) => {
                if is_account_missing(&err) {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    /// 获取 Raydium tick array，优先使用 WebSocket 订阅缓存
    pub async fn get_raydium_tick_array(
        &self,
        program_id: &Pubkey,
        pool: &Pubkey,
        start_tick_index: i32,
        current_slot: u64,
    ) -> Result<Option<Arc<raydium::TickArrayState>>> {
        let address = raydium::tick_array_address(program_id, pool, start_tick_index)
            .with_context(|| {
                format!("derive Raydium tick array for pool {pool} start {start_tick_index}")
            })?;

        // ✅ 1. 优先从订阅缓存读取
        if let Some(provider) = self.subscription_provider.read().await.as_ref() {
            if let Some(cached) = provider.get_cached(&address, current_slot).await {
                if let Ok(decoded) = raydium::decode_tick_array(&cached.data) {
                    return Ok(Some(Arc::new(decoded)));
                }
            }
        }

        // ✅ 2. 缓存未命中，回退 HTTP
        match self.rpc_http.get_account_data(&address).await {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(None);
                }
                let decoded = raydium::decode_tick_array(&data)?;
                Ok(Some(Arc::new(decoded)))
            }
            Err(err) => {
                if is_account_missing(&err) {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    /// 获取 Meteora bin array，优先使用 WebSocket 订阅缓存
    pub async fn get_meteora_bin_array(
        &self,
        pool: &Pubkey,
        index: i64,
        current_slot: u64,
    ) -> Result<Option<Arc<meteora_lb::BinArrayState>>> {
        let address = meteora_lb::bin_array_address(&METEORA_LB_PROGRAM_ID, pool, index);

        // ✅ 1. 优先从订阅缓存读取
        if let Some(provider) = self.subscription_provider.read().await.as_ref() {
            if let Some(cached) = provider.get_cached(&address, current_slot).await {
                if let Ok(decoded) = meteora_lb::decode_bin_array(&cached.data) {
                    if decoded.lb_pair == *pool {
                        return Ok(Some(Arc::new(decoded)));
                    }
                }
            }
        }

        // ✅ 2. 缓存未命中，回退 HTTP
        match self.rpc_http.get_account_data(&address).await {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(None);
                }
                let decoded = meteora_lb::decode_bin_array(&data)?;
                if decoded.lb_pair != *pool {
                    return Ok(None);
                }
                Ok(Some(Arc::new(decoded)))
            }
            Err(err) => {
                if is_account_missing(&err) {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    /// 获取 Meteora bitmap extension，优先使用 WebSocket 订阅缓存
    pub async fn get_meteora_bitmap_extension(
        &self,
        pool: &Pubkey,
        current_slot: u64,
    ) -> Result<Option<Arc<meteora_lb::BitmapExtension>>> {
        let address = meteora_lb::bitmap_extension_address(&METEORA_LB_PROGRAM_ID, pool);

        // ✅ 1. 优先从订阅缓存读取
        if let Some(provider) = self.subscription_provider.read().await.as_ref() {
            if let Some(cached) = provider.get_cached(&address, current_slot).await {
                if !cached.data.is_empty() {
                    if let Ok(decoded) = meteora_lb::decode_bitmap_extension(&cached.data) {
                        if decoded.lb_pair == *pool {
                            return Ok(Some(Arc::new(decoded)));
                        }
                    }
                }
            }
        }

        // ✅ 2. 缓存未命中，回退 HTTP
        match self.rpc_http.get_account_data(&address).await {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(None);
                }
                let decoded = meteora_lb::decode_bitmap_extension(&data)?;
                if decoded.lb_pair != *pool {
                    return Ok(None);
                }
                Ok(Some(Arc::new(decoded)))
            }
            Err(err) => {
                if is_account_missing(&err) {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    pub async fn get_pump_fun_fee_config(&self) -> Result<PumpFunFeeConfig> {
        if let Some(cached) = self.pump_fun_fees.lock().await.clone() {
            return Ok(cached);
        }

        let (global_config_key, _) =
            Pubkey::find_program_address(&[PUMP_FUN_GLOBAL_CONFIG_SEED], &PUMP_FUN_PROGRAM_ID);

        let global_data = self
            .rpc_http
            .get_account_data(&global_config_key)
            .await
            .with_context(|| format!("fetch pump.fun global config {global_config_key}"))?;

        let mut config = parse_pump_fun_global_config(&global_data)?;

        let fee_config_key = Pubkey::find_program_address(
            &[PUMP_FUN_FEE_CONFIG_SEED, &PUMP_FUN_FEE_CONFIG_STATIC_SEED],
            &PUMP_FUN_FEE_PROGRAM_ID,
        )
        .0;

        if let Ok(data) = self.rpc_http.get_account_data(&fee_config_key).await {
            if let Ok(fee_config) = parse_pump_fun_fee_config(&data) {
                config.flat_fees = fee_config.flat_fees;
                config.fee_tiers = fee_config.fee_tiers;
            }
        }

        let mut guard = self.pump_fun_fees.lock().await;
        *guard = Some(config.clone());
        Ok(config)
    }

    pub async fn get_raydium_amm_config(&self, config: &Pubkey) -> Result<RaydiumFees> {
        if let Some(cached) = self.raydium_config_cache.get(config) {
            return Ok(cached.clone());
        }

        let data = match self.rpc_http.get_account_data(config).await {
            Ok(data) => data,
            Err(err) => {
                if is_account_missing(&err) {
                    debug!(
                        "Raydium AMM config {} missing; defaulting fee rates to zero",
                        config
                    );
                    return Ok(RaydiumFees::default());
                }
                return Err(anyhow!("fetch raydium amm config {}: {}", config, err));
            }
        };

        let fees = parse_raydium_config(&data)?;
        self.raydium_config_cache.insert(*config, fees.clone());
        Ok(fees)
    }

    /// 批量获取 Raydium tick arrays（用于深档按需加载）
    ///
    /// 使用 HTTP RPC 的 getMultipleAccounts 一次性获取多个 tick array，
    /// 避免为远端深档数据创建常驻订阅。
    ///
    /// 返回成功解码的 tick arrays，缺失或解码失败的会被跳过。
    pub async fn get_raydium_tick_arrays_batch(
        &self,
        addresses: &[Pubkey],
    ) -> Result<Vec<(Pubkey, raydium::TickArrayState)>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::RpcAccountInfoConfig;

        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(self.rpc_http.commitment()),
            ..Default::default()
        };

        // 批量获取账户数据
        let accounts = match timeout(
            Duration::from_secs(5),
            self.rpc_http
                .get_multiple_ui_accounts_with_config(addresses, config),
        )
        .await
        {
            Ok(result) => result
                .with_context(|| format!("batch fetch {} Raydium tick arrays", addresses.len()))?,
            Err(_) => bail!(
                "batch fetch {} Raydium tick arrays timed out",
                addresses.len()
            ),
        };

        let mut results = Vec::new();

        for (i, maybe_account) in accounts.value.iter().enumerate() {
            if let Some(account) = maybe_account {
                if let Some(data) = account.data.decode() {
                    if let Ok(decoded) = raydium::decode_tick_array(&data) {
                        results.push((addresses[i], decoded));
                    } else {
                        debug!("Failed to decode Raydium tick array at {}", addresses[i]);
                    }
                } else {
                    debug!("Failed to decode Raydium tick array data at {}", addresses[i]);
                }
            } else {
                debug!("Raydium tick array {} not found", addresses[i]);
            }
        }

        debug!(
            "Batch fetched {} Raydium tick arrays ({}/{} successful)",
            addresses.len(),
            results.len(),
            addresses.len()
        );

        Ok(results)
    }

    /// 批量获取 Orca tick arrays（用于深档按需加载）
    ///
    /// 类似 `get_raydium_tick_arrays_batch`，但用于 Orca CLMM 池子。
    pub async fn get_orca_tick_arrays_batch(
        &self,
        addresses: &[Pubkey],
        tick_spacing: i32,
    ) -> Result<Vec<(Pubkey, orca::TickArrayState)>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::RpcAccountInfoConfig;

        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(self.rpc_http.commitment()),
            ..Default::default()
        };

        let accounts = match timeout(
            Duration::from_secs(5),
            self.rpc_http
                .get_multiple_ui_accounts_with_config(addresses, config),
        )
        .await
        {
            Ok(result) => result
                .with_context(|| format!("batch fetch {} Orca tick arrays", addresses.len()))?,
            Err(_) => bail!(
                "batch fetch {} Orca tick arrays timed out",
                addresses.len()
            ),
        };

        let mut results = Vec::new();

        for (i, maybe_account) in accounts.value.iter().enumerate() {
            if let Some(account) = maybe_account {
                if let Some(data) = account.data.decode() {
                    if let Ok(decoded) = orca::decode_tick_array(&data, tick_spacing) {
                        results.push((addresses[i], decoded));
                    } else {
                        debug!("Failed to decode Orca tick array at {}", addresses[i]);
                    }
                } else {
                    debug!("Failed to decode Orca tick array data at {}", addresses[i]);
                }
            } else {
                debug!("Orca tick array {} not found", addresses[i]);
            }
        }

        debug!(
            "Batch fetched {} Orca tick arrays ({}/{} successful)",
            addresses.len(),
            results.len(),
            addresses.len()
        );

        Ok(results)
    }

    /// 批量获取 Meteora bin arrays（用于 warmup 校验）
    ///
    /// 使用 HTTP RPC 的 getMultipleAccounts 一次性获取多个 bin array，
    /// 返回成功解码且 lb_pair 匹配的 bin arrays。
    pub async fn get_meteora_bin_arrays_batch(
        &self,
        pool: &Pubkey,
        addresses: &[Pubkey],
    ) -> Result<Vec<(Pubkey, meteora_lb::BinArrayState)>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::RpcAccountInfoConfig;

        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            commitment: Some(self.rpc_http.commitment()),
            ..Default::default()
        };

        let accounts = match timeout(
            Duration::from_secs(5),
            self.rpc_http
                .get_multiple_ui_accounts_with_config(addresses, config),
        )
        .await
        {
            Ok(result) => result
                .with_context(|| format!("batch fetch {} Meteora bin arrays", addresses.len()))?,
            Err(_) => bail!(
                "batch fetch {} Meteora bin arrays timed out",
                addresses.len()
            ),
        };

        let mut results = Vec::new();
        let expected_owner = METEORA_LB_PROGRAM_ID.to_string();

        for (i, maybe_account) in accounts.value.iter().enumerate() {
            if let Some(account) = maybe_account {
                if account.owner != expected_owner {
                    debug!(
                        "Meteora bin array {} owner mismatch: {}",
                        addresses[i], account.owner
                    );
                    continue;
                }
                if let Some(data) = account.data.decode() {
                    if let Ok(decoded) = meteora_lb::decode_bin_array(&data) {
                        if decoded.lb_pair == *pool {
                            results.push((addresses[i], decoded));
                        } else {
                            debug!(
                                "Meteora bin array {} lb_pair mismatch: {}",
                                addresses[i], decoded.lb_pair
                            );
                        }
                    } else {
                        debug!("Failed to decode Meteora bin array at {}", addresses[i]);
                    }
                } else {
                    debug!("Failed to decode Meteora bin array data at {}", addresses[i]);
                }
            } else {
                debug!("Meteora bin array {} not found", addresses[i]);
            }
        }

        debug!(
            "Batch fetched {} Meteora bin arrays ({}/{} valid)",
            addresses.len(),
            results.len(),
            addresses.len()
        );

        Ok(results)
    }
}

const PUMP_FUN_FEE_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
const PUMP_FUN_FEE_CONFIG_SEED: &[u8] = b"fee_config";
const PUMP_FUN_FEE_CONFIG_STATIC_SEED: [u8; 32] = [
    12, 20, 222, 252, 130, 94, 198, 118, 148, 37, 8, 24, 187, 101, 64, 101, 244, 41, 141, 49, 86, 213, 113,
    180, 212, 248, 9, 12, 24, 233, 168, 99,
];

fn sanitize_fee_bps(value: u64) -> u64 {
    if value > 10_000 {
        return 0;
    }
    value
}

fn parse_pump_fun_global_config(data: &[u8]) -> Result<PumpFunFeeConfig> {
    // GlobalConfig layout (from pump-public-docs):
    // 8 bytes discriminator
    // 32 bytes admin
    // 8 bytes lp_fee_basis_points (u64)
    // 8 bytes protocol_fee_basis_points (u64)
    // 8 bytes coin_creator_fee_basis_points (u64)
    // 256 bytes protocol_fee_recipients (8 * Pubkey)
    // 32 bytes admin_set_coin_creator_authority
    // 1 byte disable_flags
    const MIN_SIZE: usize = 8 + 32 + 8 + 8 + 1 + (32 * 8);
    if data.len() < MIN_SIZE {
        bail!(
            "pump.fun global config too short ({} bytes, need {})",
            data.len(),
            MIN_SIZE
        );
    }

    let mut reader = AccountReader::new(&data[8..]);
    reader.skip(32)?; // admin
    let lp_fee_bps = sanitize_fee_bps(reader.read_u64()?);
    let protocol_fee_bps = sanitize_fee_bps(reader.read_u64()?);

    let _disable_flags = reader.read_u8()?;

    // protocol_fee_recipients: 8 pubkeys
    let mut protocol_fee_recipient = None;
    for _ in 0..8 {
        let recipient = reader.read_pubkey()?;
        if protocol_fee_recipient.is_none() && recipient != Pubkey::default() {
            protocol_fee_recipient = Some(recipient);
        }
    }
    let coin_creator_fee_bps = sanitize_fee_bps(reader.read_u64().unwrap_or(0));

    log::info!(
        "Parsed PumpFun fees: lp={} protocol={} coin_creator={}",
        lp_fee_bps,
        protocol_fee_bps,
        coin_creator_fee_bps
    );

    // admin_set_coin_creator_authority (32 bytes) and disable_flags (1 byte) follow but we don't need them

    Ok(PumpFunFeeConfig {
        flat_fees: PumpFunFeeBasis {
            lp_fee_bps,
            protocol_fee_bps,
            coin_creator_fee_bps,
        },
        fee_tiers: Vec::new(),
        protocol_fee_recipient: protocol_fee_recipient
            .ok_or_else(|| anyhow!("pump.fun global config missing protocol fee recipient"))?,
    })
}

fn parse_pump_fun_fee_config(data: &[u8]) -> Result<PumpFunFeeConfig> {
    const MIN_SIZE: usize = 8 + 1 + 32 + 24 + 4;
    if data.len() < MIN_SIZE {
        bail!(
            "pump.fun fee config too short ({} bytes, need {})",
            data.len(),
            MIN_SIZE
        );
    }

    let mut reader = AccountReader::new(&data[8..]);
    reader.read_u8()?; // bump
    reader.read_pubkey()?; // admin

    let flat_fees = PumpFunFeeBasis {
        lp_fee_bps: sanitize_fee_bps(reader.read_u64()?),
        protocol_fee_bps: sanitize_fee_bps(reader.read_u64()?),
        coin_creator_fee_bps: sanitize_fee_bps(reader.read_u64()?),
    };

    let tier_count = reader.read_u32()? as usize;
    let mut fee_tiers = Vec::with_capacity(tier_count);
    for _ in 0..tier_count {
        if reader.remaining() < 16 + 24 {
            break;
        }
        let threshold = reader.read_u128()?;
        let fees = PumpFunFeeBasis {
            lp_fee_bps: sanitize_fee_bps(reader.read_u64()?),
            protocol_fee_bps: sanitize_fee_bps(reader.read_u64()?),
            coin_creator_fee_bps: sanitize_fee_bps(reader.read_u64()?),
        };
        fee_tiers.push(PumpFunFeeTier {
            market_cap_lamports_threshold: threshold,
            fees,
        });
    }

    Ok(PumpFunFeeConfig {
        flat_fees,
        fee_tiers,
        protocol_fee_recipient: Pubkey::default(),
    })
}

pub fn compute_pump_fun_market_cap_lamports(
    lp_supply_raw: u64,
    base_decimals: u8,
    price: f64,
    quote_decimals: u8,
) -> Option<u128> {
    if lp_supply_raw == 0 {
        return None;
    }
    if !(price.is_finite() && price > 0.0) {
        return None;
    }

    let supply = (lp_supply_raw as f64) / 10f64.powi(base_decimals as i32);
    if !(supply.is_finite() && supply > 0.0) {
        return None;
    }

    let market_cap_quote = supply * price;
    if !(market_cap_quote.is_finite() && market_cap_quote > 0.0) {
        return None;
    }

    let market_cap_lamports = market_cap_quote * 10f64.powi(quote_decimals as i32);
    if !(market_cap_lamports.is_finite() && market_cap_lamports > 0.0) {
        return None;
    }

    if market_cap_lamports > u128::MAX as f64 {
        return None;
    }

    Some(market_cap_lamports.floor() as u128)
}

fn parse_raydium_config(data: &[u8]) -> Result<RaydiumFees> {
    if data.len() < 8 + 1 + 2 + 32 + 4 + 4 + 2 + 4 {
        bail!(
            "raydium amm config too short ({} bytes) to decode fees",
            data.len()
        );
    }

    let mut reader = AccountReader::new(&data[8..]);
    reader.read_u8()?; // bump
    reader.read_u16()?; // index
    reader.read_pubkey()?; // owner
    let protocol_fee_rate = reader.read_u32()?;
    let trade_fee_rate = reader.read_u32()?;
    reader.read_u16()?; // tick spacing
    let fund_fee_rate = reader.read_u32()?;

    Ok(RaydiumFees {
        trade_fee_rate,
        protocol_fee_rate,
        fund_fee_rate,
    })
}

fn is_min_context_slot_not_reached(err: &ClientError) -> bool {
    let msg = err.to_string();
    msg.contains("Min context slot not reached") || msg.contains("min context slot not reached")
}

fn is_account_missing(err: &ClientError) -> bool {
    match err.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { data, .. }) => {
            matches!(data, RpcResponseErrorData::Empty)
        }
        ClientErrorKind::RpcError(RpcError::RpcRequestError(msg)) => {
            msg.contains("AccountNotFound") || msg.contains("could not find account")
        }
        _ => false,
    }
}

pub fn q64_to_price(sqrt_price_x64: u128, decimals_base: u8, decimals_quote: u8) -> Result<f64> {
    if sqrt_price_x64 == 0 {
        return Err(anyhow!("sqrt price is zero"));
    }

    let ratio = (sqrt_price_x64 as f64) / 2f64.powi(64);
    let mut price = ratio * ratio;
    let decimals_adjustment = decimals_base as i32 - decimals_quote as i32;
    if decimals_adjustment != 0 {
        price *= 10f64.powi(decimals_adjustment);
    }
    if !price.is_finite() || price <= 0.0 {
        Err(anyhow!("invalid price computed"))
    } else {
        Ok(price)
    }
}
