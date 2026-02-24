//! Pool Manager Implementation
//!
//! Periodically fetches pool lists from sidecar and maintains filtered pool set.

#![allow(dead_code)]
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use log::{debug, error, info, warn};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout, MissedTickBehavior};

use crate::constants::{
    METEORA_DAMM_V1_PROGRAM_ID, METEORA_DAMM_V2_PROGRAM_ID, METEORA_LB_PROGRAM_ID,
    RAYDIUM_CLMM_PROGRAM_ID, RAYDIUM_CPMM_PROGRAM_ID,
};
use crate::dex::{raydium_amm, DexKind, PoolDescriptor};
use crate::sidecar_client::{MeteoraPairInfo, PumpTokenInfo, SidecarClient};

// Pump.fun program ID (mainnet default, can be overridden via PUMP_PROGRAM_ID env)
const DEFAULT_PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

// Meteora DLMM program ID for classification
const METEORA_DLMM_PROGRAM_ID: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

// External HTTP APIs for pool discovery
const METEORA_API_URL: &str = "https://dlmm-api.meteora.ag/pair/all";
const DEXSCREENER_TOKEN_URL: &str = "https://api.dexscreener.com/latest/dex/tokens";
const SOL_MINT_STR: &str = "So11111111111111111111111111111111111111112";

// Pump.fun API endpoints (deprecated - use Moralis instead)
const PUMP_FUN_API_BASE: &str = "https://frontend-api.pump.fun";

// Moralis API for Pump.fun token discovery
const MORALIS_API_BASE: &str = "https://solana-gateway.moralis.io";

fn get_moralis_api_key() -> Option<String> {
    std::env::var("MORALIS_API_KEY").ok()
}

fn get_dexscreener_delay_ms() -> u64 {
    std::env::var("DEXSCREENER_DELAY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

fn get_dexscreener_min_liquidity() -> f64 {
    std::env::var("DEXSCREENER_MIN_LIQUIDITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000.0)
}

fn get_meteora_min_tvl() -> f64 {
    std::env::var("METEORA_MIN_TVL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000.0)
}

fn get_output_min_liquidity() -> f64 {
    std::env::var("OUTPUT_MIN_LIQUIDITY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000.0)
}

fn get_pump_min_usd_market_cap() -> f64 {
    std::env::var("PUMP_MIN_USD_MARKET_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000.0) // Default: 10K USD market cap
}

fn get_pump_api_limit() -> usize {
    std::env::var("PUMP_API_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200) // Default: fetch top 200 coins
}

#[derive(Debug, Deserialize)]
struct MeteoraApiPair {
    address: String,
    name: String,
    #[serde(rename = "mint_x")]
    mint_x: String,
    #[serde(rename = "mint_y")]
    mint_y: String,
    liquidity: String,
    #[serde(default)]
    trade_volume_24h: f64,
}

#[derive(Debug)]
struct MeteoraSolPool {
    pair_address: String,
    base_mint: String,
    symbol: String,
    tvl_usd: f64,
    volume_24h_usd: f64,
}

#[derive(Debug, Deserialize)]
struct DexToken {
    address: String,
    #[serde(default)]
    symbol: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DexLiquidity {
    usd: f64,
}

#[derive(Debug, Deserialize)]
struct DexVolume {
    #[serde(rename = "h24")]
    h24: f64,
}

#[derive(Debug, Deserialize)]
struct DexScreenerPair {
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "dexId")]
    dex_id: String,
    #[serde(rename = "pairAddress")]
    pair_address: String,
    #[serde(rename = "baseToken")]
    base_token: DexToken,
    #[serde(rename = "quoteToken")]
    quote_token: DexToken,
    #[serde(default)]
    liquidity: Option<DexLiquidity>,
    #[serde(default)]
    volume: Option<DexVolume>,
}

#[derive(Debug, Deserialize)]
struct DexScreenerResponse {
    pairs: Option<Vec<DexScreenerPair>>,
}

#[derive(Debug)]
struct OtherDexPoolInfo {
    pair_address: String,
    dex_id: String,
    liquidity_usd: f64,
    volume_24h_usd: f64,
}

// ==================== Moralis API Types ====================

/// Moralis Pump.fun token response
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MoralisPumpToken {
    token_address: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    liquidity: Option<String>,
    #[serde(default)]
    fully_diluted_valuation: Option<String>,
    #[serde(default)]
    bonding_curve_progress: Option<f64>,
    #[serde(default)]
    graduated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MoralisResponse {
    result: Vec<MoralisPumpToken>,
    #[serde(default)]
    cursor: Option<String>,
}

// ==================== Pump.fun API Types (legacy) ====================

/// Pump.fun coin from frontend-api /coins endpoint (deprecated - use Moralis)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
struct PumpFunCoin {
    mint: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    bonding_curve: Option<String>,
    #[serde(default)]
    raydium_pool: Option<String>,
    #[serde(default)]
    complete: Option<bool>,
    #[serde(default)]
    virtual_sol_reserves: Option<u64>,
    #[serde(default)]
    market_cap: Option<f64>,
    #[serde(default)]
    usd_market_cap: Option<f64>,
}

/// Get Pump program ID from env or use default
fn get_pump_program_id() -> Pubkey {
    let id_str = std::env::var("PUMP_PROGRAM_ID")
        .unwrap_or_else(|_| DEFAULT_PUMP_PROGRAM_ID.to_string());
    
    Pubkey::from_str(&id_str).unwrap_or_else(|_| {
        warn!(
            "[PoolManager] Invalid PUMP_PROGRAM_ID '{}', using default",
            id_str
        );
        Pubkey::from_str(DEFAULT_PUMP_PROGRAM_ID).unwrap()
    })
}

use super::config::PoolFilterConfig;
use super::types::PoolMeta;

/// Result of a pool refresh operation
#[derive(Debug, Clone)]
pub struct RefreshResult {
    /// Number of candidate pools from sidecar
    pub candidates: usize,
    /// Number of pools after basic filtering
    pub filtered: usize,
    /// Number of pools after pair intersection filter (if enabled)
    pub intersection_filtered: usize,
    /// Number of newly added pools
    pub added: usize,
    /// Number of removed pools
    pub removed: usize,
    /// Pubkeys of newly added pools
    pub added_keys: Vec<Pubkey>,
    /// Pubkeys of removed pools
    pub removed_keys: Vec<Pubkey>,
}

/// Pool Manager: manages dynamic pool list
pub struct PoolManager {
    /// Filter configuration
    config: PoolFilterConfig,
    /// Pump.fun program ID (cached from env)
    pump_program_id: Pubkey,
    /// Current pool set (protected by RwLock for concurrent access)
    pools: Arc<RwLock<HashMap<Pubkey, PoolMeta>>>,
    /// Last successful refresh time
    last_refresh: Arc<RwLock<Option<Instant>>>,
    /// Whether the manager has been initialized
    initialized: Arc<RwLock<bool>>,
    /// RPC client for on-chain queries (pool owner classification)
    rpc_client: Arc<RpcClient>,
    /// Sidecar client for Pump.fun token discovery and other list endpoints
    sidecar_client: Arc<SidecarClient>,
    /// Runtime blacklist of pools that failed during monitoring (quarantined)
    runtime_blacklist: Arc<RwLock<HashSet<Pubkey>>>,
}

impl PoolManager {
    /// Create a new PoolManager from environment
    pub fn from_env() -> Result<Self> {
        let config = PoolFilterConfig::from_env();
        let pump_program_id = get_pump_program_id();
        
        // Get RPC URL from environment
        let rpc_url = std::env::var("SOLANA_HTTP_URL")
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
        let rpc_client = Arc::new(RpcClient::new(rpc_url.clone()));
        let sidecar_client = Arc::new(SidecarClient::from_env()?);
        
        info!(
            "[PoolManager] Using PUMP_PROGRAM_ID: {}, RPC: {}",
            pump_program_id,
            rpc_url
        );

        Ok(Self {
            config,
            pump_program_id,
            pools: Arc::new(RwLock::new(HashMap::new())),
            last_refresh: Arc::new(RwLock::new(None)),
            initialized: Arc::new(RwLock::new(false)),
            rpc_client,
            sidecar_client,
            runtime_blacklist: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Create a new PoolManager with explicit config and clients
    pub fn new(
        config: PoolFilterConfig,
        rpc_client: Arc<RpcClient>,
        sidecar_client: Arc<SidecarClient>,
    ) -> Self {
        let pump_program_id = get_pump_program_id();
        
        Self {
            config,
            pump_program_id,
            pools: Arc::new(RwLock::new(HashMap::new())),
            last_refresh: Arc::new(RwLock::new(None)),
            initialized: Arc::new(RwLock::new(false)),
            rpc_client,
            sidecar_client,
            runtime_blacklist: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Add a pool to the runtime blacklist so it will be excluded on the next refresh
    pub async fn blacklist_pool(&self, key: &Pubkey) {
        let mut guard = self.runtime_blacklist.write().await;
        guard.insert(*key);
    }

    /// Get the filter configuration
    pub fn config(&self) -> &PoolFilterConfig {
        &self.config
    }

    /// Get the refresh interval
    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs(self.config.refresh_secs)
    }

    /// Check if the manager has been initialized (at least one successful refresh)
    pub async fn is_initialized(&self) -> bool {
        *self.initialized.read().await
    }

    /// Get a read-only snapshot of current pools
    pub async fn get_pools(&self) -> HashMap<Pubkey, PoolMeta> {
        self.pools.read().await.clone()
    }

    /// Get pool count
    pub async fn pool_count(&self) -> usize {
        self.pools.read().await.len()
    }

    /// Get a specific pool by key
    pub async fn get_pool(&self, key: &Pubkey) -> Option<PoolMeta> {
        self.pools.read().await.get(key).cloned()
    }

    /// Fetch hot meme token arbitrage pools directly from Meteora API + DexScreener
    ///
    /// This mirrors the logic from the TypeScript sidecar's pump-list.ts:
    /// 1) Fetch all Meteora DLMM pools, filter to SOL pairs with TVL >= METEORA_MIN_TVL
    /// 2) Dedupe by base token (keep highest TVL pool per token)
    /// 3) For each token, fetch SOL pools on non-Meteora DEXes from DexScreener
    /// 4) Build a combined pool list (Meteora + other DEXes)
    /// 5) Apply OUTPUT_MIN_LIQUIDITY, group by base mint, keep only groups with >= 2 pools
    async fn fetch_dexscreener_pools_direct(&self) -> Result<Vec<MeteoraPairInfo>> {
        let client = reqwest::Client::new();

        // Step 1: Meteora DLMM SOL pools
        let min_tvl = get_meteora_min_tvl();
        info!(
            "[PoolManager] Fetching Meteora DLMM pools from {} (min TVL ${})",
            METEORA_API_URL, min_tvl
        );

        let resp = client
            .get(METEORA_API_URL)
            .timeout(Duration::from_secs(60))
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!(
                "[PoolManager] Meteora API error: status {}",
                resp.status()
            );
            return Ok(Vec::new());
        }

        let all_pairs: Vec<MeteoraApiPair> = resp.json().await?;
        info!("[PoolManager] Meteora total pairs fetched: {}", all_pairs.len());

        // Filter to SOL pairs with min TVL and dedupe by base mint
        let sol_mint = SOL_MINT_STR;
        let mut by_base: HashMap<String, MeteoraSolPool> = HashMap::new();

        for pair in all_pairs {
            let is_sol_pair = pair.mint_x == sol_mint || pair.mint_y == sol_mint;
            if !is_sol_pair {
                continue;
            }

            let tvl: f64 = pair.liquidity.parse().unwrap_or(0.0);
            if tvl < min_tvl {
                continue;
            }

            let base_mint = if pair.mint_x == sol_mint {
                pair.mint_y.clone()
            } else {
                pair.mint_x.clone()
            };

            let symbol = pair
                .name
                .split('-')
                .next()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| base_mint.chars().take(8).collect());

            let entry = by_base.entry(base_mint.clone()).or_insert(MeteoraSolPool {
                pair_address: pair.address.clone(),
                base_mint: base_mint.clone(),
                symbol,
                tvl_usd: tvl,
                volume_24h_usd: pair.trade_volume_24h,
            });

            if tvl > entry.tvl_usd {
                entry.pair_address = pair.address;
                entry.tvl_usd = tvl;
                entry.volume_24h_usd = pair.trade_volume_24h;
            }
        }

        let meteora_pools: Vec<MeteoraSolPool> = by_base.into_values().collect();
        info!(
            "[PoolManager] Meteora SOL pairs with TVL >= ${}: {}",
            min_tvl,
            meteora_pools.len()
        );

        if meteora_pools.is_empty() {
            return Ok(Vec::new());
        }

        // Step 2: For each token, find SOL pools on non-Meteora DEXes from DexScreener
        let delay_ms = get_dexscreener_delay_ms();
        let min_liq = get_dexscreener_min_liquidity();
        info!(
            "[PoolManager] Fetching DexScreener pools (delay={}ms, min_liq=${})",
            delay_ms,
            min_liq
        );

        let mut pairs: Vec<MeteoraPairInfo> = Vec::new();

        for (idx, pool) in meteora_pools.iter().enumerate() {
            let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, pool.base_mint);
            let resp = client
                .get(&url)
                .timeout(Duration::from_secs(10))
                .send()
                .await;

            let mut other_pools: Vec<OtherDexPoolInfo> = Vec::new();

            if let Ok(resp) = resp {
                if resp.status().is_success() {
                    if let Ok(body) = resp.json::<DexScreenerResponse>().await {
                        if let Some(list) = body.pairs {
                            for p in list {
                                let liq = p
                                    .liquidity
                                    .as_ref()
                                    .map(|l| l.usd)
                                    .unwrap_or(0.0);
                                let vol = p
                                    .volume
                                    .as_ref()
                                    .map(|v| v.h24)
                                    .unwrap_or(0.0);

                                // Include all Solana SOL pools from supported DEXes
                                // Note: We now include "meteora" pools from DexScreener too,
                                // as they may be DAMM (DYN/DYN2) pools not in the DLMM API.
                                // Deduplication happens later by pair_address.
                                if p.chain_id == "solana"
                                    && p.quote_token.address == sol_mint
                                    && liq >= min_liq
                                {
                                    other_pools.push(OtherDexPoolInfo {
                                        pair_address: p.pair_address,
                                        dex_id: p.dex_id,
                                        liquidity_usd: liq,
                                        volume_24h_usd: vol,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Sort by liquidity descending
            other_pools.sort_by(|a, b| {
                b.liquidity_usd
                    .partial_cmp(&a.liquidity_usd)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            // Always include the Meteora pool in the library
            pairs.push(MeteoraPairInfo {
                pair_address: pool.pair_address.clone(),
                base_mint: pool.base_mint.clone(),
                quote_mint: SOL_MINT_STR.to_string(),
                tvl_usd: Some(pool.tvl_usd),
                volume_24h_usd: Some(pool.volume_24h_usd),
                label: Some(format!("{} (meteora)", pool.symbol)),
                dex_id: Some("meteora".to_string()),
            });

            // Include all qualifying other-DEX pools
            for other in other_pools {
                pairs.push(MeteoraPairInfo {
                    pair_address: other.pair_address,
                    base_mint: pool.base_mint.clone(),
                    quote_mint: SOL_MINT_STR.to_string(),
                    tvl_usd: Some(other.liquidity_usd),
                    volume_24h_usd: Some(other.volume_24h_usd),
                    label: Some(format!("{} ({})", pool.symbol, other.dex_id)),
                    dex_id: Some(other.dex_id),
                });
            }

            if idx + 1 < meteora_pools.len() {
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        // Apply OUTPUT_MIN_LIQUIDITY filter and group by base mint
        let min_output_liq = get_output_min_liquidity();
        let mut grouped: HashMap<String, Vec<MeteoraPairInfo>> = HashMap::new();

        for p in pairs.into_iter() {
            let tvl = p.tvl_usd.unwrap_or(0.0);
            if tvl < min_output_liq {
                continue;
            }
            grouped.entry(p.base_mint.clone()).or_default().push(p);
        }

        // Sort each group by TVL desc and keep only groups with >= 2 pools
        let mut result: Vec<MeteoraPairInfo> = Vec::new();
        let mut total_groups = 0usize;
        let mut paired_groups = 0usize;

        for (_base, mut list) in grouped {
            total_groups += 1;
            list.sort_by(|a, b| {
                let atvl = a.tvl_usd.unwrap_or(0.0);
                let btvl = b.tvl_usd.unwrap_or(0.0);
                btvl
                    .partial_cmp(&atvl)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            if list.len() >= 2 {
                paired_groups += 1;
                result.extend(list);
            }
        }

        info!(
            "[PoolManager] DexScreener grouping: total groups={}, groups>=2={}, pools kept={}",
            total_groups,
            paired_groups,
            result.len()
        );

        Ok(result)
    }

    /// Fetch Pump.fun coins directly from Pump.fun frontend API
    ///
    /// This fetches coins from https://frontend-api.pump.fun/coins with sorting by market cap,
    /// then filters by minimum USD market cap and converts to PumpTokenInfo for processing.
    ///
    /// Note: Pump.fun's frontend-api may be unreliable. If this fails, the PoolManager
    /// will fall back to sidecar, and pumpswap pools are also discovered via DexScreener
    /// (dex_id="pumpswap") when fetching Meteora token pools.
    async fn fetch_pump_fun_coins_direct(&self) -> Result<Vec<PumpTokenInfo>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
        let limit = get_pump_api_limit();
        let min_market_cap = get_pump_min_usd_market_cap();

        debug!(
            "[PoolManager] Attempting Pump.fun direct API (limit={}, min_market_cap=${})",
            limit, min_market_cap
        );

        let mut all_coins: Vec<PumpFunCoin> = Vec::new();

        // Strategy 1: Fetch coins sorted by market cap
        let url = format!(
            "{}/coins?sort=market_cap&order=DESC&limit={}&offset=0&includeNsfw=false",
            PUMP_FUN_API_BASE, limit
        );

        match client
            .get(&url)
            .header("Accept", "application/json")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    match resp.json::<Vec<PumpFunCoin>>().await {
                        Ok(coins) => {
                            info!(
                                "[PoolManager] Pump.fun /coins returned {} coins",
                                coins.len()
                            );
                            all_coins.extend(coins);
                        }
                        Err(err) => {
                            debug!(
                                "[PoolManager] Failed to parse Pump.fun /coins response: {:?}",
                                err
                            );
                        }
                    }
                } else {
                    debug!(
                        "[PoolManager] Pump.fun /coins returned status {} (API may be unavailable)",
                        status
                    );
                }
            }
            Err(err) => {
                debug!("[PoolManager] Pump.fun API request failed: {:?}", err);
            }
        }

        // If we got no coins from the main endpoint, return empty
        // The caller will fall back to sidecar or rely on DexScreener pumpswap discovery
        if all_coins.is_empty() {
            debug!("[PoolManager] Pump.fun direct API returned no coins");
            return Ok(Vec::new());
        }

        // Deduplicate by mint
        let mut seen_mints: HashSet<String> = HashSet::new();
        all_coins.retain(|coin| seen_mints.insert(coin.mint.clone()));

        // Convert to PumpTokenInfo and filter by market cap
        let mut result: Vec<PumpTokenInfo> = Vec::new();

        for coin in all_coins {
            let usd_mcap = coin.usd_market_cap.or(coin.market_cap).unwrap_or(0.0);

            if usd_mcap < min_market_cap {
                continue;
            }

            let bonding_state = if coin.complete.unwrap_or(false) || coin.raydium_pool.is_some() {
                Some(crate::sidecar_client::PumpBondingState::Graduated)
            } else {
                Some(crate::sidecar_client::PumpBondingState::Bonding)
            };

            // Estimate liquidity from virtual reserves
            let liquidity_usd = coin.virtual_sol_reserves.map(|sol_lamports| {
                let sol = sol_lamports as f64 / 1_000_000_000.0;
                sol * 150.0 * 2.0
            });

            result.push(PumpTokenInfo {
                mint: coin.mint,
                bonding_curve: coin.bonding_curve,
                symbol: coin.symbol,
                name: coin.name,
                liquidity_usd,
                marketcap_usd: Some(usd_mcap),
                volume_24h_usd: None,
                bonding_state,
            });
        }

        // Sort by market cap descending
        result.sort_by(|a, b| {
            let a_mcap = a.marketcap_usd.unwrap_or(0.0);
            let b_mcap = b.marketcap_usd.unwrap_or(0.0);
            b_mcap.partial_cmp(&a_mcap).unwrap_or(std::cmp::Ordering::Equal)
        });

        info!(
            "[PoolManager] Pump.fun direct API: {} coins after market cap filter (>= ${})",
            result.len(),
            min_market_cap
        );

        Ok(result)
    }

    /// Fetch Pump.fun graduated tokens from Moralis API (recommended method)
    ///
    /// This fetches all graduated Pump tokens with pagination, filters by liquidity,
    /// and returns them as a list of token mints for DexScreener lookup.
    async fn fetch_pump_tokens_from_moralis(&self, min_liquidity: f64) -> Result<Vec<String>> {
        let api_key = match get_moralis_api_key() {
            Some(key) => key,
            None => {
                debug!("[PoolManager] MORALIS_API_KEY not set, skipping Moralis fetch");
                return Ok(Vec::new());
            }
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let mut all_tokens: Vec<MoralisPumpToken> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut page = 0;
        let limit = 100;
        let max_pages = 50; // Safety limit

        info!(
            "[PoolManager] Fetching Pump tokens from Moralis (min_liq=${})",
            min_liquidity
        );

        loop {
            page += 1;

            let url = match &cursor {
                Some(c) => format!(
                    "{}/token/mainnet/exchange/pumpfun/graduated?limit={}&cursor={}",
                    MORALIS_API_BASE, limit, c
                ),
                None => format!(
                    "{}/token/mainnet/exchange/pumpfun/graduated?limit={}",
                    MORALIS_API_BASE, limit
                ),
            };

            let resp = client
                .get(&url)
                .header("X-API-Key", &api_key)
                .header("Accept", "application/json")
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    match r.json::<MoralisResponse>().await {
                        Ok(data) => {
                            let count = data.result.len();
                            all_tokens.extend(data.result);

                            if count == 0 {
                                break;
                            }

                            if let Some(next_cursor) = data.cursor {
                                if next_cursor.is_empty() {
                                    break;
                                }
                                cursor = Some(next_cursor);
                            } else {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("[PoolManager] Failed to parse Moralis response: {:?}", e);
                            break;
                        }
                    }
                }
                Ok(r) => {
                    let status = r.status();
                    let text = r.text().await.unwrap_or_default();
                    warn!("[PoolManager] Moralis API error {}: {}", status, text);
                    break;
                }
                Err(e) => {
                    warn!("[PoolManager] Moralis request failed: {:?}", e);
                    break;
                }
            }

            // Rate limit
            sleep(Duration::from_millis(100)).await;

            if page >= max_pages {
                info!("[PoolManager] Reached Moralis page limit ({})", max_pages);
                break;
            }
        }

        info!(
            "[PoolManager] Fetched {} total Pump tokens from Moralis ({} pages)",
            all_tokens.len(),
            page
        );

        // Filter by liquidity and extract mints
        let filtered: Vec<String> = all_tokens
            .into_iter()
            .filter(|t| {
                let liq = t.liquidity.as_ref()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                liq >= min_liquidity
            })
            .map(|t| t.token_address)
            .collect();

        info!(
            "[PoolManager] {} Pump tokens with liquidity >= ${}",
            filtered.len(),
            min_liquidity
        );

        Ok(filtered)
    }

    /// Fetch Meteora tokens from their API
    ///
    /// Returns a list of ALL token mints that have Meteora pools paired with SOL.
    /// TVL filtering is now done later in the DexScreener step.
    async fn fetch_meteora_tokens(&self, _min_tvl: f64) -> Result<Vec<String>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(90))  // 增加超时：Meteora API 返回 127K+ pairs
            .build()?;

        info!("[PoolManager] Fetching ALL Meteora tokens (TVL filter deferred to DexScreener)");

        let resp = client
            .get(METEORA_API_URL)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("Meteora API returned {}", resp.status());
        }

        let pairs: Vec<MeteoraApiPair> = resp.json().await?;
        info!("[PoolManager] Meteora API returned {} pairs", pairs.len());

        // Pre-filter by TVL >= $20,000 to reduce tokens sent to DexScreener
        // (stricter $50,000 filter applied later in Step 3.5)
        const METEORA_PRE_FILTER_TVL: f64 = 20_000.0;
        let sol_mint = SOL_MINT_STR;
        let mut seen_mints: HashSet<String> = HashSet::new();
        let mut result: Vec<String> = Vec::new();

        for pair in pairs {
            // Check TVL first
            let tvl: f64 = pair.liquidity.parse().unwrap_or(0.0);
            if tvl < METEORA_PRE_FILTER_TVL {
                continue;
            }

            // Determine base mint (non-SOL side)
            let base_mint = if pair.mint_x == sol_mint {
                pair.mint_y.clone()
            } else if pair.mint_y == sol_mint {
                pair.mint_x.clone()
            } else {
                continue; // Not a SOL pair
            };

            if seen_mints.insert(base_mint.clone()) {
                result.push(base_mint);
            }
        }

        info!(
            "[PoolManager] {} unique Meteora tokens (wSOL pairs with TVL >= ${})",
            result.len(),
            METEORA_PRE_FILTER_TVL
        );

        Ok(result)
    }

    /// Fetch all pools for a token from DexScreener
    ///
    /// Returns ALL pools from all DEXes (Meteora, Raydium, Orca, Pumpswap, etc.)
    /// wSOL + TVL filtering is done later in refresh() step 3.5
    async fn fetch_dexscreener_pools_for_token(
        &self,
        client: &reqwest::Client,
        token_mint: &str,
        _min_liquidity: f64,  // 不再在这里筛选，而是在 Step 3.5 统一筛选
    ) -> Vec<MeteoraPairInfo> {
        let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, token_mint);

        let resp = match client
            .get(&url)
            .header("Accept", "application/json")
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        if !resp.status().is_success() {
            return Vec::new();
        }

        let data: DexScreenerResponse = match resp.json().await {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        let mut result: Vec<MeteoraPairInfo> = Vec::new();

        if let Some(pairs) = data.pairs {
            for p in pairs {
                // Only Solana pairs (wSOL + TVL filter done later)
                if p.chain_id != "solana" {
                    continue;
                }

                let liq = p
                    .liquidity
                    .as_ref()
                    .map(|l| l.usd)
                    .unwrap_or(0.0);
                let vol = p
                    .volume
                    .as_ref()
                    .map(|v| v.h24)
                    .unwrap_or(0.0);

                let symbol = p.base_token.symbol.unwrap_or_else(|| "???".to_string());
                let label = format!("{} ({})", symbol, p.dex_id);

                result.push(MeteoraPairInfo {
                    pair_address: p.pair_address,
                    base_mint: p.base_token.address,
                    quote_mint: p.quote_token.address,  // 保留实际的 quote mint，不强制为 SOL
                    tvl_usd: Some(liq),
                    volume_24h_usd: Some(vol),
                    label: Some(label),
                    dex_id: Some(p.dex_id),
                });
            }
        }

        result
    }

    /// Fetch Pumpswap pools directly from DexScreener
    ///
    /// This discovers pumpswap pools independently of Meteora, by querying DexScreener
    /// for top Solana pairs and filtering for dex_id="pumpswap".
    async fn fetch_pumpswap_pools_from_dexscreener(&self) -> Result<Vec<MeteoraPairInfo>> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let min_liq = get_dexscreener_min_liquidity();
        let limit = get_pump_api_limit();

        info!(
            "[PoolManager] Fetching Pumpswap pools from DexScreener (min_liq=${}, limit={})",
            min_liq, limit
        );

        let mut all_pairs: Vec<MeteoraPairInfo> = Vec::new();

        // Strategy 1: Use DexScreener token profiles/boosts to find trending tokens
        // then check if they have pumpswap pools
        let boosts_url = "https://api.dexscreener.com/token-boosts/top/v1";
        
        match client
            .get(boosts_url)
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                // Parse boosted tokens response
                #[derive(Debug, Deserialize)]
                struct BoostToken {
                    #[serde(rename = "tokenAddress")]
                    token_address: String,
                    #[serde(rename = "chainId")]
                    chain_id: String,
                }

                if let Ok(tokens) = resp.json::<Vec<BoostToken>>().await {
                    let solana_tokens: Vec<_> = tokens
                        .into_iter()
                        .filter(|t| t.chain_id == "solana")
                        .take(limit / 2) // Take half from boosts
                        .collect();

                    info!(
                        "[PoolManager] Found {} boosted Solana tokens from DexScreener",
                        solana_tokens.len()
                    );

                    // Query each token for pumpswap pools
                    for (idx, token) in solana_tokens.iter().enumerate() {
                        if let Ok(pairs) = self.fetch_pumpswap_for_token(&client, &token.token_address, min_liq).await {
                            all_pairs.extend(pairs);
                        }

                        // Rate limit
                        if idx + 1 < solana_tokens.len() {
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
            _ => {
                debug!("[PoolManager] DexScreener boosts endpoint unavailable");
            }
        }

        // Strategy 2: Use DexScreener search for "pumpswap" pairs directly
        // Note: This endpoint may have limitations
        let search_url = "https://api.dexscreener.com/latest/dex/search?q=SOL";
        
        match client
            .get(search_url)
            .header("Accept", "application/json")
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(data) = resp.json::<DexScreenerResponse>().await {
                    if let Some(pairs) = data.pairs {
                        for p in pairs {
                            if p.chain_id == "solana" 
                                && p.dex_id == "pumpswap"
                                && p.quote_token.address == SOL_MINT_STR
                            {
                                let liq = p.liquidity.as_ref().map(|l| l.usd).unwrap_or(0.0);
                                let vol = p.volume.as_ref().map(|v| v.h24).unwrap_or(0.0);

                                if liq >= min_liq {
                                    let symbol = p.base_token.symbol.unwrap_or_else(|| "???".to_string());
                                    all_pairs.push(MeteoraPairInfo {
                                        pair_address: p.pair_address,
                                        base_mint: p.base_token.address,
                                        quote_mint: SOL_MINT_STR.to_string(),
                                        tvl_usd: Some(liq),
                                        volume_24h_usd: Some(vol),
                                        label: Some(format!("{} (pumpswap)", symbol)),
                                        dex_id: Some("pumpswap".to_string()),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                debug!("[PoolManager] DexScreener search endpoint unavailable");
            }
        }

        // Deduplicate by pair_address
        let mut seen: HashSet<String> = HashSet::new();
        all_pairs.retain(|p| seen.insert(p.pair_address.clone()));

        // Sort by TVL descending
        all_pairs.sort_by(|a, b| {
            let a_tvl = a.tvl_usd.unwrap_or(0.0);
            let b_tvl = b.tvl_usd.unwrap_or(0.0);
            b_tvl.partial_cmp(&a_tvl).unwrap_or(std::cmp::Ordering::Equal)
        });

        info!(
            "[PoolManager] Discovered {} unique Pumpswap pools from DexScreener",
            all_pairs.len()
        );

        Ok(all_pairs)
    }

    /// Helper: Fetch pumpswap pools for a specific token from DexScreener
    async fn fetch_pumpswap_for_token(
        &self,
        client: &reqwest::Client,
        token_address: &str,
        min_liq: f64,
    ) -> Result<Vec<MeteoraPairInfo>> {
        let url = format!("{}/{}", DEXSCREENER_TOKEN_URL, token_address);
        
        let resp = client
            .get(&url)
            .header("Accept", "application/json")
            .timeout(Duration::from_secs(10))
            .send()
            .await;

        let mut result = Vec::new();

        if let Ok(resp) = resp {
            if resp.status().is_success() {
                if let Ok(data) = resp.json::<DexScreenerResponse>().await {
                    if let Some(pairs) = data.pairs {
                        for p in pairs {
                            if p.chain_id == "solana"
                                && p.dex_id == "pumpswap"
                                && p.quote_token.address == SOL_MINT_STR
                            {
                                let liq = p.liquidity.as_ref().map(|l| l.usd).unwrap_or(0.0);
                                let vol = p.volume.as_ref().map(|v| v.h24).unwrap_or(0.0);

                                if liq >= min_liq {
                                    let symbol = p.base_token.symbol.unwrap_or_else(|| "???".to_string());
                                    result.push(MeteoraPairInfo {
                                        pair_address: p.pair_address,
                                        base_mint: p.base_token.address,
                                        quote_mint: SOL_MINT_STR.to_string(),
                                        tvl_usd: Some(liq),
                                        volume_24h_usd: Some(vol),
                                        label: Some(format!("{} (pumpswap)", symbol)),
                                        dex_id: Some("pumpswap".to_string()),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Convert current pools to PoolDescriptors for monitor integration
    pub async fn to_descriptors(&self) -> Vec<PoolDescriptor> {
        self.pools
            .read()
            .await
            .values()
            .map(|meta| PoolDescriptor {
                label: meta.label.clone().unwrap_or_else(|| meta.key.to_string()),
                address: meta.key,
                kind: meta.dex,
            })
            .collect()
    }

    /// Perform a refresh: fetch tokens from Pump (Moralis) and Meteora in parallel,
    /// then use DexScreener to find all pools for each token, and build arbitrage pairs.
    ///
    /// Returns the refresh result with statistics, or error if fetch failed.
    /// On fetch failure, the existing pool set is preserved.
    pub async fn refresh(&self) -> Result<RefreshResult> {
        info!("[PoolManager] Starting refresh...");

        let min_liquidity = get_dexscreener_min_liquidity();
        info!("[PoolManager] Min liquidity threshold: ${}", min_liquidity);

        // ==================== Step 1: Fetch token lists from Meteora ====================
        // Moralis Pump token fetch removed - DexScreener will find PumpFun pools via Meteora tokens
        let meteora_result = self.fetch_meteora_tokens(min_liquidity).await;

        let meteora_tokens = meteora_result.unwrap_or_else(|e| {
            warn!("[PoolManager] Failed to fetch Meteora tokens: {:?}", e);
            Vec::new()
        });

        // Use Meteora tokens directly (no more Moralis merge)
        let unique_mints: Vec<String> = meteora_tokens;
        info!(
            "[PoolManager] {} unique token mints to query DexScreener",
            unique_mints.len()
        );

        // ==================== Step 2: Query DexScreener for all pools ====================
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()?;

        let delay_ms = get_dexscreener_delay_ms();
        let mut all_pairs: Vec<MeteoraPairInfo> = Vec::new();

        info!(
            "[PoolManager] Querying DexScreener for pools (delay={}ms)...",
            delay_ms
        );

        for (idx, mint) in unique_mints.iter().enumerate() {
            let pools = match timeout(
                Duration::from_secs(12),
                self.fetch_dexscreener_pools_for_token(&client, mint, min_liquidity),
            )
            .await
            {
                Ok(p) => p,
                Err(_) => {
                    warn!(
                        "[PoolManager] DexScreener timeout for mint {} ({}/{})",
                        mint,
                        idx + 1,
                        unique_mints.len()
                    );
                    Vec::new()
                }
            };
            all_pairs.extend(pools);

            // Rate limit
            if idx + 1 < unique_mints.len() {
                sleep(Duration::from_millis(delay_ms)).await;
            }

            // Progress log every 50 tokens
            if (idx + 1) % 50 == 0 {
                info!(
                    "[PoolManager] Progress: {}/{} tokens queried, {} pools found",
                    idx + 1,
                    unique_mints.len(),
                    all_pairs.len()
                );
            }
        }

        info!(
            "[PoolManager] DexScreener returned {} total pools for {} tokens",
            all_pairs.len(),
            unique_mints.len()
        );

        // ==================== Step 3: Deduplicate pools by address ====================
        let mut seen_addresses: HashSet<String> = HashSet::new();
        all_pairs.retain(|p| seen_addresses.insert(p.pair_address.clone()));

        info!(
            "[PoolManager] {} unique pools after deduplication",
            all_pairs.len()
        );

        // ==================== Step 3.5: Filter by wSOL + TVL ====================
        let min_liq = get_dexscreener_min_liquidity();
        let before_filter = all_pairs.len();
        let mut dropped_non_sol = 0usize;
        let mut dropped_low_tvl = 0usize;
        let mut filtered_pairs: Vec<MeteoraPairInfo> = Vec::with_capacity(all_pairs.len());
        for pair in all_pairs.into_iter() {
            let is_sol_pair = pair.quote_mint == SOL_MINT_STR;
            let meets_tvl = pair.tvl_usd.unwrap_or(0.0) >= min_liq;
            if !is_sol_pair {
                dropped_non_sol += 1;
                continue;
            }
            if !meets_tvl {
                dropped_low_tvl += 1;
                continue;
            }
            filtered_pairs.push(pair);
        }
        all_pairs = filtered_pairs;
        info!(
            "[PoolManager] {} pools after wSOL + TVL filter (dropped={}, non_wsol={}, low_tvl={}, min_tvl=${})",
            all_pairs.len(),
            before_filter - all_pairs.len(),
            dropped_non_sol,
            dropped_low_tvl,
            min_liq
        );

        // ==================== Step 4: Convert to PoolMeta ====================
        let mut seen_keys: HashSet<Pubkey> = HashSet::new();
        let mut candidates: Vec<PoolMeta> = Vec::new();
        let mut dex_counts: HashMap<DexKind, usize> = HashMap::new();
        let mut dropped_config_filter = 0usize;
        let mut dropped_convert_failed = 0usize;

        // Separate Meteora pools (need RPC classification) from others
        let mut meteora_pairs: Vec<MeteoraPairInfo> = Vec::new();
        let mut other_pairs: Vec<MeteoraPairInfo> = Vec::new();

        for pair in all_pairs {
            if !self.config.filter_meteora_pair(&pair) {
                dropped_config_filter += 1;
                continue;
            }
            match pair.dex_id.as_deref() {
                Some("meteora") => meteora_pairs.push(pair),
                _ => other_pairs.push(pair),
            }
        }

        // Process non-Meteora pools first (no RPC needed)
        for pair in other_pairs {
            match self.convert_meteora_pair(&pair).await {
                Some(meta) => {
                    if seen_keys.insert(meta.key) {
                        *dex_counts.entry(meta.dex).or_insert(0) += 1;
                        candidates.push(meta);
                    }
                }
                None => {
                    dropped_convert_failed += 1;
                }
            }
        }

        // Process Meteora pools (need RPC classification)
        let meteora_count = meteora_pairs.len();
        if meteora_count > 0 {
            info!(
                "[PoolManager] Classifying {} Meteora pools by on-chain owner...",
                meteora_count
            );

            const BATCH_SIZE: usize = 10;
            const BATCH_DELAY_MS: u64 = 50;

            for (batch_idx, batch) in meteora_pairs.chunks(BATCH_SIZE).enumerate() {
                let futures: Vec<_> = batch
                    .iter()
                    .map(|pair| self.convert_meteora_pair(pair))
                    .collect();

                let results = futures::future::join_all(futures).await;

                for meta in results {
                    match meta {
                        Some(meta) => {
                            if seen_keys.insert(meta.key) {
                                *dex_counts.entry(meta.dex).or_insert(0) += 1;
                                candidates.push(meta);
                            }
                        }
                        None => {
                            dropped_convert_failed += 1;
                        }
                    }
                }

                if batch_idx + 1 < (meteora_count + BATCH_SIZE - 1) / BATCH_SIZE {
                    sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
                }
            }
        }

        info!(
            "[PoolManager] Converted {} pools to candidates",
            candidates.len()
        );
        if !dex_counts.is_empty() {
            let mut entries: Vec<(String, usize)> = dex_counts
                .into_iter()
                .map(|(dex, count)| (dex.to_string(), count))
                .collect();
            entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let summary = entries
                .into_iter()
                .map(|(name, count)| format!("{}={}", name, count))
                .collect::<Vec<_>>()
                .join(", ");
            info!("[PoolManager] Candidates by DEX: {}", summary);
        }

        // ==================== Step 5: Apply blacklists ====================
        // Apply config and runtime blacklists before pair-level filters
        let mut dropped_blacklist = 0usize;
        if !self.config.pool_blacklist.is_empty() || !self.runtime_blacklist.read().await.is_empty() {
            let cfg_blacklist: HashSet<Pubkey> = self.config.pool_blacklist.iter().cloned().collect();
            let rt_blacklist = self.runtime_blacklist.read().await;
            let before_blacklist = candidates.len();
            candidates.retain(|meta| {
                !cfg_blacklist.contains(&meta.key) && !rt_blacklist.contains(&meta.key)
            });
            dropped_blacklist = before_blacklist.saturating_sub(candidates.len());
        }

        let filtered_count = candidates.len();
        let candidate_count = filtered_count;

        // Apply pair intersection filter if enabled
        let before_intersection = candidates.len();
        let mut candidates = if self.config.pair_intersection_only {
            self.apply_pair_intersection_filter(candidates)
        } else {
            candidates
        };

        let intersection_count = candidates.len();
        let dropped_pair_intersection = before_intersection.saturating_sub(intersection_count);

        // 进一步按"币对"维度过滤：只保留至少包含 2 个池子的币对（可同 DEX 多池）。
        // 丢弃单池币对；同时过滤掉 24h 成交量不足的币对。
        {
            use std::collections::HashMap;

            let min_pair_vol24h = self.config.min_vol24h_usd;

            let mut pair_groups: HashMap<(Pubkey, Pubkey), Vec<PoolMeta>> = HashMap::new();
            for pool in candidates.iter() {
                let key = if pool.base_mint < pool.quote_mint {
                    (pool.base_mint, pool.quote_mint)
                } else {
                    (pool.quote_mint, pool.base_mint)
                };
                pair_groups.entry(key).or_default().push(pool.clone());
            }

            let mut pruned: Vec<PoolMeta> = Vec::new();
            let mut dropped_single_pool: usize = 0;
            let mut dropped_low_volume: usize = 0;

            for (_pair_key, group) in pair_groups {
                // 先按单个池子的交易量过滤
                let qualified_pools: Vec<PoolMeta> = group
                    .into_iter()
                    .filter(|p| {
                        let vol = p.vol24h_usd.unwrap_or(0.0);
                        if min_pair_vol24h > 0.0 && vol < min_pair_vol24h {
                            false // 单个池子交易量不足，丢弃
                        } else {
                            true
                        }
                    })
                    .collect();

                // 过滤后，检查是否还有 2 个以上的池子
                if qualified_pools.len() < 2 {
                    if qualified_pools.is_empty() {
                        dropped_low_volume += 1; // 所有池子都因交易量不足被丢弃
                    } else {
                        dropped_single_pool += 1; // 过滤后只剩 1 个池子
                    }
                    continue;
                }

                // 池子数量达标，保留
                pruned.extend(qualified_pools);
            }

            if dropped_single_pool > 0 || dropped_low_volume > 0 {
                info!(
                    "[PoolManager] Dropped {} single-pool pairs, {} low-volume pairs (min_vol24h={})",
                    dropped_single_pool, dropped_low_volume, min_pair_vol24h
                );
            }

            candidates = pruned;

            let mut drop_reasons: Vec<String> = Vec::new();
            if dropped_single_pool > 0 {
                drop_reasons.push(format!("single_pool_pairs={}", dropped_single_pool));
            }
            if dropped_low_volume > 0 {
                drop_reasons.push(format!("low_volume_pairs={}", dropped_low_volume));
            }
            if !drop_reasons.is_empty() {
                info!(
                    "[PoolManager] Pair-level drop summary: {}",
                    drop_reasons.join(", ")
                );
            }
        }

        // Apply global cap on dynamic pool count (always at pair level)
        let mut dropped_pair_cap = 0usize;
        if let Some(max_pools) = self.config.max_dynamic_pools {
            let before = candidates.len();
            candidates = self.apply_pair_cap(candidates, max_pools);
            dropped_pair_cap = before.saturating_sub(candidates.len());
            info!(
                "[PoolManager] Applying max_dynamic_pools pair-cap: {} -> {} pools (max_dynamic_pools={})",
                before,
                candidates.len(),
                max_pools,
            );
        }

        // Calculate diff and update
        let new_keys: HashSet<Pubkey> = candidates.iter().map(|m| m.key).collect();
        let result = {
            let mut pools = self.pools.write().await;
            let old_keys: HashSet<Pubkey> = pools.keys().cloned().collect();

            let added_keys: Vec<Pubkey> = new_keys.difference(&old_keys).cloned().collect();
            let removed_keys: Vec<Pubkey> = old_keys.difference(&new_keys).cloned().collect();

            let added = added_keys.len();
            let removed = removed_keys.len();

            // Remove old pools
            for key in &removed_keys {
                pools.remove(key);
            }

            // Add/update new pools
            for meta in candidates {
                pools.insert(meta.key, meta);
            }

            RefreshResult {
                candidates: candidate_count,
                filtered: filtered_count,
                intersection_filtered: intersection_count,
                added,
                removed,
                added_keys,
                removed_keys,
            }
        };

        // Update refresh time and initialized flag
        *self.last_refresh.write().await = Some(Instant::now());
        *self.initialized.write().await = true;

        if self.config.pair_intersection_only && filtered_count != intersection_count {
            info!(
                "[PoolManager] Refresh complete: candidates={}, filtered={}, intersection={}, added={}, removed={}",
                result.candidates, result.filtered, result.intersection_filtered, result.added, result.removed
            );
        } else {
            info!(
                "[PoolManager] Refresh complete: candidates={}, filtered={}, added={}, removed={}",
                result.candidates, result.filtered, result.added, result.removed
            );
        }

        let mut drop_reasons: Vec<String> = Vec::new();
        if dropped_non_sol > 0 {
            drop_reasons.push(format!("non_wsol={}", dropped_non_sol));
        }
        if dropped_low_tvl > 0 {
            drop_reasons.push(format!("low_tvl={}", dropped_low_tvl));
        }
        if dropped_config_filter > 0 {
            drop_reasons.push(format!("config_filter={}", dropped_config_filter));
        }
        if dropped_convert_failed > 0 {
            drop_reasons.push(format!("convert_failed={}", dropped_convert_failed));
        }
        if dropped_blacklist > 0 {
            drop_reasons.push(format!("blacklist={}", dropped_blacklist));
        }
        if dropped_pair_intersection > 0 {
            drop_reasons.push(format!("pair_intersection_pools={}", dropped_pair_intersection));
        }
        if dropped_pair_cap > 0 {
            drop_reasons.push(format!("pair_cap_pools={}", dropped_pair_cap));
        }
        if !drop_reasons.is_empty() {
            info!(
                "[PoolManager] Drop summary: {}",
                drop_reasons.join(", ")
            );
        }

        Ok(result)
    }

    /// Classify a Meteora pool by its on-chain program owner
    ///
    /// Returns the correct DexKind based on the pool's owner program:
    /// - METEORA_DAMM_V1_PROGRAM_ID -> None (deprecated, skip)
    /// - METEORA_DAMM_V2_PROGRAM_ID -> MeteoraDammV2
    /// - METEORA_LB_PROGRAM_ID -> MeteoraLb (same as DLMM program)
    /// - METEORA_DLMM_PROGRAM_ID -> MeteoraDlmm
    async fn classify_meteora_pool_by_owner(&self, pool_address: &Pubkey) -> Option<DexKind> {
        let fetch_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.rpc_client.get_account(pool_address),
        )
        .await;

        match fetch_result {
            Ok(Ok(account)) => {
                let owner = account.owner;
                if owner == METEORA_DAMM_V1_PROGRAM_ID {
                    // ⭐ DAMM V1 已弃用，返回 None 跳过这些池子
                    debug!(
                        "[PoolManager] Pool {} is MeteoraDammV1, skipping (deprecated)",
                        pool_address
                    );
                    None
                } else if owner == METEORA_DAMM_V2_PROGRAM_ID {
                    debug!("[PoolManager] Pool {} classified as MeteoraDammV2 (owner={})", pool_address, owner);
                    Some(DexKind::MeteoraDammV2)
                } else if owner == METEORA_LB_PROGRAM_ID {
                    // LB program is the same as DLMM program
                    debug!("[PoolManager] Pool {} classified as MeteoraDlmm (owner={})", pool_address, owner);
                    Some(DexKind::MeteoraDlmm)
                } else if owner.to_string() == METEORA_DLMM_PROGRAM_ID {
                    debug!("[PoolManager] Pool {} classified as MeteoraDlmm (owner={})", pool_address, owner);
                    Some(DexKind::MeteoraDlmm)
                } else {
                    warn!(
                        "[PoolManager] Pool {} has unknown Meteora owner {}, skipping",
                        pool_address, owner
                    );
                    None
                }
            }
            Ok(Err(err)) => {
                warn!(
                    "[PoolManager] Failed to fetch account {} for classification: {:?}",
                    pool_address, err
                );
                None
            }
            Err(_) => {
                warn!(
                    "[PoolManager] Fetch account {} for classification timed out",
                    pool_address
                );
                None
            }
        }
    }

    /// Classify a Raydium pool by its on-chain program owner (CLMM vs CPMM vs AMM V4)
    async fn classify_raydium_pool_by_owner(&self, pool_address: &Pubkey) -> Option<DexKind> {
        let fetch_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.rpc_client.get_account(pool_address),
        )
        .await;

        match fetch_result {
            Ok(Ok(account)) => {
                let owner = account.owner;
                if owner == RAYDIUM_CLMM_PROGRAM_ID {
                    debug!(
                        "[PoolManager] Pool {} classified as RaydiumClmm (owner={})",
                        pool_address, owner
                    );
                    Some(DexKind::RaydiumClmm)
                } else if owner == RAYDIUM_CPMM_PROGRAM_ID {
                    debug!(
                        "[PoolManager] Pool {} classified as RaydiumCpmm (owner={})",
                        pool_address, owner
                    );
                    Some(DexKind::RaydiumCpmm)
                } else if owner.to_string() == raydium_amm::RAYDIUM_AMM_V4_PROGRAM_ID {
                    debug!(
                        "[PoolManager] Pool {} classified as RaydiumAmmV4 (owner={})",
                        pool_address, owner
                    );
                    Some(DexKind::RaydiumAmmV4)
                } else {
                    warn!(
                        "[PoolManager] Pool {} has unknown Raydium owner {}, skipping",
                        pool_address, owner
                    );
                    None
                }
            }
            Ok(Err(err)) => {
                warn!(
                    "[PoolManager] Failed to fetch account {} for Raydium classification: {:?}",
                    pool_address, err
                );
                None
            }
            Err(_) => {
                warn!(
                    "[PoolManager] Fetch account {} for Raydium classification timed out",
                    pool_address
                );
                None
            }
        }
    }

    /// Convert Meteora pair info to PoolMeta (async version)
    /// 
    /// Maps dex_id from DexScreener to correct DexKind:
    /// - "meteora" -> Classify by on-chain owner (DLMM/DAMM V1/DAMM V2/LB)
    /// - "raydium" -> RaydiumClmm
    /// - "orca" -> OrcaWhirlpool
    /// - "pumpswap" -> Pump.fun DLMM
    /// - Others -> Skip (not supported)
    async fn convert_meteora_pair(&self, pair: &MeteoraPairInfo) -> Option<PoolMeta> {
        let key = Pubkey::from_str(&pair.pair_address).ok()?;
        let base_mint = Pubkey::from_str(&pair.base_mint).ok()?;
        let quote_mint = Pubkey::from_str(&pair.quote_mint).ok()?;

        // Map dex_id to DexKind
        let dex_kind = match pair.dex_id.as_deref() {
            Some("meteora") => {
                // For Meteora pools, classify by on-chain owner to distinguish DLMM/DAMM/LB
                self.classify_meteora_pool_by_owner(&key).await?
            }
            Some("raydium") => {
                // For Raydium pools, classify by on-chain owner to distinguish CLMM vs AMM V4
                self.classify_raydium_pool_by_owner(&key).await?
            }
            Some("orca") => DexKind::OrcaWhirlpool,
            Some("pumpswap") | Some("pumpfun") | Some("pump_fun") => DexKind::PumpFunDlmm,
            // Default: try to classify by owner
            None => {
                self.classify_meteora_pool_by_owner(&key).await?
            }
            // Skip unsupported DEXes
            Some(other) => {
                debug!("[PoolManager] Skipping unsupported DEX '{}' for pool {}", other, pair.pair_address);
                return None;
            }
        };

        Some(PoolMeta::new(
            dex_kind,
            key,
            base_mint,
            quote_mint,
            pair.tvl_usd,
            pair.volume_24h_usd,
            pair.label.clone(),
        ))
    }

    /// Convert Pump token info to PoolMeta
    ///
    #[allow(dead_code)]
    fn convert_pump_token(&self, token: &PumpTokenInfo) -> Option<PoolMeta> {
        let bonding_curve = token.bonding_curve.as_ref()?;
        let key = Pubkey::from_str(bonding_curve).ok()?;
        let base_mint = Pubkey::from_str(&token.mint).ok()?;
        let quote_mint = Pubkey::from_str(SOL_MINT_STR).ok()?;
        let label = token
            .symbol
            .as_ref()
            .or(token.name.as_ref())
            .map(|s| format!("{} (pumpswap)", s));

        Some(PoolMeta::new(
            DexKind::PumpFunDlmm,
            key,
            base_mint,
            quote_mint,
            token.liquidity_usd,
            token.volume_24h_usd,
            label,
        ))
    }

    /// Apply pair intersection filter: only keep pairs that exist on all required DEXes
    ///
    /// Groups pools by normalized pair (sorted base/quote mints), then keeps only
    /// groups that contain pools from every DEX in `pair_intersection_dexes`.
    fn apply_pair_intersection_filter(&self, candidates: Vec<PoolMeta>) -> Vec<PoolMeta> {
        use std::collections::HashMap;

        let required_dexes = &self.config.pair_intersection_dexes;
        
        // Group pools by normalized pair key
        // Normalized key: sorted (min(base,quote), max(base,quote))
        let mut pair_groups: HashMap<(Pubkey, Pubkey), Vec<PoolMeta>> = HashMap::new();
        
        for pool in candidates {
            let normalized_key = if pool.base_mint < pool.quote_mint {
                (pool.base_mint, pool.quote_mint)
            } else {
                (pool.quote_mint, pool.base_mint)
            };
            pair_groups.entry(normalized_key).or_default().push(pool);
        }

        let mut kept_pairs = 0;
        let mut dropped_pairs = 0;
        let mut result: Vec<PoolMeta> = Vec::new();

        for (pair_key, pools) in pair_groups {
            // Collect DEX kinds present for this pair
            let dexes_present: HashSet<DexKind> = pools.iter().map(|p| p.dex).collect();
            
            // Check if all required DEXes are present
            if required_dexes.iter().all(|d| dexes_present.contains(d)) {
                kept_pairs += 1;
                result.extend(pools);
            } else {
                dropped_pairs += 1;
                debug!(
                    "[PoolManager] Pair ({}, {}) dropped: missing DEXes {:?}",
                    pair_key.0,
                    pair_key.1,
                    required_dexes.difference(&dexes_present).collect::<Vec<_>>()
                );
            }
        }

        info!(
            "[PoolManager] Pair intersection: kept {} pairs ({} pools), dropped {} pairs",
            kept_pairs,
            result.len(),
            dropped_pairs
        );

        result
    }

    /// Apply a global cap at the "pair" level when pair_intersection_only is enabled.
    ///
    /// Assumes all incoming pools already satisfy the pair intersection constraint.
    /// Groups pools by normalized pair, computes an aggregate TVL score per pair
    /// (sum of TVL across all DEX pools for that pair), then selects whole pairs
    /// in descending TVL order while keeping the total pool count under `max_pools`.
    ///
    /// This guarantees that we never leave "half" pairs in the dynamic set: every
    /// retained pool belongs to some arbitrage pair with all required DEXes present.
    fn apply_pair_cap(&self, candidates: Vec<PoolMeta>, max_pools: usize) -> Vec<PoolMeta> {
        use std::cmp::Ordering;
        use std::collections::HashMap;

        if max_pools == 0 {
            return Vec::new();
        }

        // Group by normalized pair key again
        let mut pair_groups: HashMap<(Pubkey, Pubkey), Vec<PoolMeta>> = HashMap::new();
        for pool in candidates {
            let normalized_key = if pool.base_mint < pool.quote_mint {
                (pool.base_mint, pool.quote_mint)
            } else {
                (pool.quote_mint, pool.base_mint)
            };
            pair_groups.entry(normalized_key).or_default().push(pool);
        }

        // Turn into a sortable vector with aggregate TVL per pair
        struct PairGroup {
            _key: (Pubkey, Pubkey),
            pools: Vec<PoolMeta>,
            tvl_sum: f64,
        }

        let mut groups: Vec<PairGroup> = pair_groups
            .into_iter()
            .map(|(key, pools)| {
                let tvl_sum: f64 = pools
                    .iter()
                    .map(|p| p.tvl_usd.unwrap_or(0.0))
                    .sum();
                PairGroup { _key: key, pools, tvl_sum }
            })
            .collect();

        // Sort by descending TVL sum so we prefer more liquid pairs
        groups.sort_by(|a, b| {
            b.tvl_sum
                .partial_cmp(&a.tvl_sum)
                .unwrap_or(Ordering::Equal)
        });

        let mut result: Vec<PoolMeta> = Vec::new();
        let mut total_pools: usize = 0;
        let mut kept_pairs: usize = 0;
        let mut dropped_pairs: usize = 0;

        for group in groups {
            let group_len = group.pools.len();

            // If adding this whole pair group would exceed the max, skip it.
            if total_pools + group_len > max_pools {
                dropped_pairs += 1;
                continue;
            }

            total_pools += group_len;
            kept_pairs += 1;
            result.extend(group.pools);

            if total_pools >= max_pools {
                break;
            }
        }

        info!(
            "[PoolManager] Pair-cap: kept {} pairs ({} pools) under max_dynamic_pools={}, dropped {} pairs",
            kept_pairs,
            total_pools,
            max_pools,
            dropped_pairs
        );

        result
    }

    /// Derive bonding curve PDA from mint
    /// Seeds: ['bonding-curve', mint]
    fn derive_bonding_curve_pda(&self, mint: &Pubkey) -> Pubkey {
        let (bonding_curve, _bump) = Pubkey::find_program_address(
            &[b"bonding-curve", mint.as_ref()],
            &self.pump_program_id,
        );
        bonding_curve
    }

    /// Start a background refresh task
    ///
    /// Returns a handle that can be used to stop the task.
    pub fn spawn_refresh_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let refresh_interval = self.refresh_interval();

        tokio::spawn(async move {
            info!(
                "[PoolManager] Background refresh task started (interval: {:?})",
                refresh_interval
            );

            // Initial refresh with retry
            let mut retry_count = 0;
            const MAX_INITIAL_RETRIES: usize = 5;
            const RETRY_DELAY: Duration = Duration::from_secs(5);

            loop {
                match self.refresh().await {
                    Ok(result) => {
                        info!(
                            "[PoolManager] Initial refresh succeeded: {} pools",
                            result.filtered
                        );
                        break;
                    }
                    Err(err) => {
                        retry_count += 1;
                        if retry_count >= MAX_INITIAL_RETRIES {
                            error!(
                                "[PoolManager] Initial refresh failed after {} retries: {:?}",
                                retry_count, err
                            );
                            // Continue anyway, will retry on next interval
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

            // Periodic refresh loop
            let mut interval = tokio::time::interval(refresh_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                interval.tick().await;

                match self.refresh().await {
                    Ok(result) => {
                        if result.added > 0 || result.removed > 0 {
                            info!(
                                "[PoolManager] Periodic refresh: +{} -{} (total: {})",
                                result.added, result.removed, result.filtered
                            );
                        } else {
                            debug!(
                                "[PoolManager] Periodic refresh: no changes (total: {})",
                                result.filtered
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            "[PoolManager] Periodic refresh failed (keeping old pools): {:?}",
                            err
                        );
                        // Don't crash, just keep old pools
                    }
                }
            }
        })
    }
}

