//! Pool Manager Types

use std::time::Instant;

use solana_pubkey::Pubkey;

use crate::dex::DexKind;

/// Unified pool metadata from both Meteora DLMM and Pump.fun sources
#[derive(Debug, Clone)]
pub struct PoolMeta {
    /// DEX type (MeteoraDlmm or PumpFunDlmm)
    pub dex: DexKind,
    /// Pool address (pair address for Meteora, bonding curve for Pump)
    pub key: Pubkey,
    /// Base token mint
    pub base_mint: Pubkey,
    /// Quote token mint
    pub quote_mint: Pubkey,
    /// Total value locked in USD (if available)
    pub tvl_usd: Option<f64>,
    /// 24-hour trading volume in USD (if available)
    pub vol24h_usd: Option<f64>,
    /// Display label (e.g., "SOL-USDC")
    pub label: Option<String>,
    /// Last time this pool was updated from sidecar
    pub last_updated: Instant,
}

impl PoolMeta {
    /// Create a new PoolMeta with current timestamp
    pub fn new(
        dex: DexKind,
        key: Pubkey,
        base_mint: Pubkey,
        quote_mint: Pubkey,
        tvl_usd: Option<f64>,
        vol24h_usd: Option<f64>,
        label: Option<String>,
    ) -> Self {
        Self {
            dex,
            key,
            base_mint,
            quote_mint,
            tvl_usd,
            vol24h_usd,
            label,
            last_updated: Instant::now(),
        }
    }

    /// Update the last_updated timestamp
    pub fn touch(&mut self) {
        self.last_updated = Instant::now();
    }
}
