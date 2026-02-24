//! Pool Filter Configuration
//!
//! Loads filter rules from environment variables.

use std::collections::HashSet;
use std::str::FromStr;

use log::{debug, info, warn};
use solana_pubkey::Pubkey;

use crate::dex::DexKind;
use crate::sidecar_client::{MeteoraPairInfo, PumpTokenInfo};

/// Well-known token mints for default quote whitelist
pub mod known_mints {
    /// SOL (wrapped)
    pub const SOL: &str = "So11111111111111111111111111111111111111112";
    /// USDC
    pub const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    /// USDT
    pub const USDT: &str = "Es9vMFrzaCER9R7y5Yh8G1xM9F5pg6C4i3fXo4x3yFhQ";
}

/// Pool filter configuration loaded from environment
#[derive(Debug, Clone)]
pub struct PoolFilterConfig {
    // ==================== General Filters ====================
    /// Quote token whitelist (only pools with these quote mints pass)
    pub quote_whitelist: HashSet<Pubkey>,
    /// Minimum TVL in USD (default 0)
    pub min_tvl_usd: f64,
    /// Minimum 24h volume in USD (default 0)
    pub min_vol24h_usd: f64,

    // ==================== White/Black Lists ====================
    /// Pool whitelist (always include these, bypass filters)
    pub pool_whitelist: HashSet<Pubkey>,
    /// Pool blacklist (always exclude these)
    pub pool_blacklist: HashSet<Pubkey>,

    // ==================== Refresh Settings ====================
    /// Refresh interval in seconds (default 300)
    pub refresh_secs: u64,

    // ==================== Cross-DEX Pair Intersection ====================
    /// Only keep pairs that exist on all specified DEXes (default false)
    pub pair_intersection_only: bool,
    /// DEX set for pair intersection (default: {MeteoraDlmm, OrcaWhirlpool})
    pub pair_intersection_dexes: HashSet<DexKind>,

    // ==================== Dynamic Pool Count Limit ====================
    /// Optional hard cap on number of dynamic pools to monitor (None = no cap)
    pub max_dynamic_pools: Option<usize>,
}

impl Default for PoolFilterConfig {
    fn default() -> Self {
        Self {
            quote_whitelist: default_quote_whitelist(),
            min_tvl_usd: 0.0,
            min_vol24h_usd: 0.0,
            pool_whitelist: HashSet::new(),
            pool_blacklist: HashSet::new(),
            refresh_secs: 300,
            pair_intersection_only: false,
            pair_intersection_dexes: default_intersection_dexes(),
            max_dynamic_pools: None,
        }
    }
}

impl PoolFilterConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // POOL_QUOTE_WHITELIST
        if let Ok(raw) = std::env::var("POOL_QUOTE_WHITELIST") {
            config.quote_whitelist = parse_pubkey_list(&raw);
            if config.quote_whitelist.is_empty() {
                warn!("POOL_QUOTE_WHITELIST parsed to empty set, using defaults");
                config.quote_whitelist = default_quote_whitelist();
            }
        }

        // MIN_TVL_USD
        if let Ok(raw) = std::env::var("MIN_TVL_USD") {
            config.min_tvl_usd = raw.parse().unwrap_or(0.0);
        }

        // MIN_VOL24H_USD
        if let Ok(raw) = std::env::var("MIN_VOL24H_USD") {
            config.min_vol24h_usd = raw.parse().unwrap_or(0.0);
        }

        // POOL_WHITELIST
        if let Ok(raw) = std::env::var("POOL_WHITELIST") {
            config.pool_whitelist = parse_pubkey_list(&raw);
        }

        // POOL_BLACKLIST
        if let Ok(raw) = std::env::var("POOL_BLACKLIST") {
            config.pool_blacklist = parse_pubkey_list(&raw);
        }

        // POOL_REFRESH_SECS
        if let Ok(raw) = std::env::var("POOL_REFRESH_SECS") {
            config.refresh_secs = raw.parse().unwrap_or(300);
        }

        // PAIR_INTERSECTION_ONLY
        if let Ok(raw) = std::env::var("PAIR_INTERSECTION_ONLY") {
            config.pair_intersection_only = matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }

        // PAIR_INTERSECTION_DEXES
        if let Ok(raw) = std::env::var("PAIR_INTERSECTION_DEXES") {
            let parsed = parse_dex_list(&raw);
            if parsed.len() >= 2 {
                config.pair_intersection_dexes = parsed;
            } else {
                warn!(
                    "PAIR_INTERSECTION_DEXES needs at least 2 DEXes, got {}, using default",
                    parsed.len()
                );
            }
        }

        // MAX_DYNAMIC_POOLS
        if let Ok(raw) = std::env::var("MAX_DYNAMIC_POOLS") {
            match raw.parse::<usize>() {
                Ok(n) if n > 0 => {
                    config.max_dynamic_pools = Some(n);
                }
                Ok(_) => {
                    warn!("MAX_DYNAMIC_POOLS must be > 0, got {} - ignoring", raw);
                }
                Err(err) => {
                    warn!("Failed to parse MAX_DYNAMIC_POOLS='{}': {:?}", raw, err);
                }
            }
        }

        info!(
            "[PoolFilter] Config loaded: quote_whitelist={} mints, min_tvl={}, min_vol={}, refresh={}s, pair_intersection_only={}, max_dynamic_pools={:?}",
            config.quote_whitelist.len(),
            config.min_tvl_usd,
            config.min_vol24h_usd,
            config.refresh_secs,
            config.pair_intersection_only,
            config.max_dynamic_pools,
        );

        if config.pair_intersection_only {
            let dex_names: Vec<_> = config.pair_intersection_dexes.iter().map(|d| format!("{:?}", d)).collect();
            info!("[PoolFilter] Pair intersection DEXes: {:?}", dex_names);
        }

        config
    }

    /// Check if a Meteora pair passes the filter
    pub fn filter_meteora_pair(&self, pair: &MeteoraPairInfo) -> bool {
        // Parse pubkeys
        let pair_key = match Pubkey::from_str(&pair.pair_address) {
            Ok(pk) => pk,
            Err(_) => {
                debug!("Invalid pair_address: {}", pair.pair_address);
                return false;
            }
        };

        let base_mint = match Pubkey::from_str(&pair.base_mint) {
            Ok(pk) => pk,
            Err(_) => {
                debug!("Invalid base_mint: {}", pair.base_mint);
                return false;
            }
        };

        let quote_mint = match Pubkey::from_str(&pair.quote_mint) {
            Ok(pk) => pk,
            Err(_) => {
                debug!("Invalid quote_mint: {}", pair.quote_mint);
                return false;
            }
        };

        // Blacklist check (highest priority)
        if self.pool_blacklist.contains(&pair_key) {
            debug!("Pool {} blacklisted", pair.pair_address);
            return false;
        }

        // Whitelist check (bypass all other filters)
        if self.pool_whitelist.contains(&pair_key) {
            debug!("Pool {} whitelisted, bypassing filters", pair.pair_address);
            return true;
        }

        // Quote mint whitelist check
        if !self.quote_whitelist.contains(&quote_mint) && !self.quote_whitelist.contains(&base_mint)
        {
            debug!(
                "Pool {} quote mint not in whitelist (base={}, quote={})",
                pair.pair_address, pair.base_mint, pair.quote_mint
            );
            return false;
        }

        // Volume filtering is handled at the pair level (PoolManager) to avoid per-pool drop.

        true
    }

    /// Check if a Pump.fun token passes the filter
    pub fn filter_pump_token(&self, token: &PumpTokenInfo) -> bool {
        // Pump token 走与其它 DEX 相同的过滤逻辑：
        //  - 先应用黑/白名单
        //  - 再按全局 MIN_TVL_USD / MIN_VOL24H_USD 做体量过滤

        let mint = match Pubkey::from_str(&token.mint) {
            Ok(pk) => pk,
            Err(_) => {
                debug!("Invalid mint: {}", token.mint);
                return false;
            }
        };

        // Blacklist
        if self.pool_blacklist.contains(&mint) {
            debug!("Token {} blacklisted", token.mint);
            return false;
        }

        // Whitelist bypasses all other filters
        if self.pool_whitelist.contains(&mint) {
            debug!("Token {} whitelisted, bypassing filters", token.mint);
            return true;
        }

        // 使用 token.liquidity_usd 作为 Pump 池的 TVL 近似，走 MIN_TVL_USD
        if let Some(liq) = token.liquidity_usd {
            if liq < self.min_tvl_usd {
                debug!(
                    "Token {} liquidity {} < global min_tvl_usd {}",
                    token.mint, liq, self.min_tvl_usd
                );
                return false;
            }
        }

        // 使用通用 MIN_VOL24H_USD 过滤 Pump 24 小时成交额
        if let Some(vol) = token.volume_24h_usd {
            if vol < self.min_vol24h_usd {
                debug!(
                    "Token {} vol24h {} < min_vol24h_usd {}",
                    token.mint, vol, self.min_vol24h_usd
                );
                return false;
            }
        }

        true
    }
}

/// Parse comma-separated pubkey list
fn parse_pubkey_list(raw: &str) -> HashSet<Pubkey> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| match Pubkey::from_str(s) {
            Ok(pk) => Some(pk),
            Err(_) => {
                warn!("Invalid pubkey in list: {}", s);
                None
            }
        })
        .collect()
}

/// Default quote whitelist: SOL, USDC, USDT
fn default_quote_whitelist() -> HashSet<Pubkey> {
    [known_mints::SOL, known_mints::USDC, known_mints::USDT]
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect()
}

/// Default DEX set for pair intersection: MeteoraDlmm + OrcaWhirlpool
/// NOTE: PumpFunDlmm excluded due to high fees (1.2%+) making arbitrage unprofitable
fn default_intersection_dexes() -> HashSet<DexKind> {
    [DexKind::MeteoraDlmm, DexKind::OrcaWhirlpool]
        .into_iter()
        .collect()
}

/// Parse DEX kind from string (case-insensitive)
fn parse_dex_kind(s: &str) -> Option<DexKind> {
    match s.trim().to_ascii_lowercase().as_str() {
        "meteora_dlmm" | "meteoradlmm" | "meteora" => Some(DexKind::MeteoraDlmm),
        "pump_fun_dlmm" | "pumpfundlmm" | "pump" | "pumpfun" => Some(DexKind::PumpFunDlmm),
        "orca_whirlpool" | "orcawhirlpool" | "orca" => Some(DexKind::OrcaWhirlpool),
        "raydium_clmm" | "raydiumclmm" | "raydium" => Some(DexKind::RaydiumClmm),
        "meteora_lb" | "meteoralb" => Some(DexKind::MeteoraLb),
        "meteora_damm_v1" | "meteoradammv1" => Some(DexKind::MeteoraDammV1),
        "meteora_damm_v2" | "meteoradammv2" => Some(DexKind::MeteoraDammV2),
        _ => {
            warn!("Unknown DEX kind: {}", s);
            None
        }
    }
}

/// Parse comma-separated DEX list
fn parse_dex_list(raw: &str) -> HashSet<DexKind> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(parse_dex_kind)
        .collect()
}

