use anyhow::{anyhow, Context, Result};
use log::{debug, warn};
use reqwest::Client;
use solana_instruction::{AccountMeta as SolanaAccountMeta, Instruction};
use solana_pubkey::Pubkey;
use std::str::FromStr;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::time::{timeout, Duration};
use once_cell::sync::Lazy;

use super::types::*;

/// 全局信号量：限制并发 sidecar 调用数量，避免 429 限流
/// 最多允许 3 个并发调用
static SIDECAR_SEMAPHORE: Lazy<Semaphore> = Lazy::new(|| Semaphore::new(3));

/// Client for communicating with the TypeScript sidecar service
#[derive(Debug, Clone)]
pub struct SidecarClient {
    base_url: String,
    client: Client,
}

impl SidecarClient {
    /// Create a new sidecar client from environment variable SIDECAR_URL
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("SIDECAR_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
        Self::new(base_url)
    }

    /// Create a new sidecar client with explicit base URL
    pub fn new(base_url: String) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // 5 minutes for large pool discovery
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self { base_url, client })
    }

    /// Acquire semaphore permit before making sidecar call
    async fn acquire_permit(&self) -> tokio::sync::SemaphorePermit<'static> {
        SIDECAR_SEMAPHORE.acquire().await.expect("semaphore closed")
    }

    // ==================== Meteora DLMM Methods ====================

    /// Get a quote for a Meteora DLMM swap
    pub async fn quote_meteora_dlmm(
        &self,
        pool: &str,
        side: Side,
        amount_in: u64,
        user: Option<&str>,
    ) -> Result<MeteoraQuoteResponse> {
        let request = MeteoraQuoteRequest {
            pool: pool.to_string(),
            side,
            amount_in: amount_in.to_string(),
            user: user.map(|s| s.to_string()),
        };

        let url = format!("{}/meteora/quote", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send quote request")?;

        let response: SidecarResponse<MeteoraQuoteResponse> = response
            .json()
            .await
            .context("Failed to parse quote response")?;

        response.into_result()
    }

    /// Build a Meteora DLMM swap instruction
    pub async fn build_meteora_dlmm_swap_ix(
        &self,
        pool: &str,
        side: Side,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        user_token_in: Option<&Pubkey>,
        user_token_out: Option<&Pubkey>,
    ) -> Result<Instruction> {
        let request = MeteoraSwapIxRequest {
            pool: pool.to_string(),
            side,
            amount_in: amount_in.to_string(),
            min_amount_out: min_amount_out.to_string(),
            user: user.to_string(),
            user_token_in: user_token_in.map(|p| p.to_string()),
            user_token_out: user_token_out.map(|p| p.to_string()),
        };

        let url = format!("{}/meteora/build_swap_ix", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send build_swap_ix request")?;

        let response: SidecarResponse<MeteoraSwapIxResponse> = response
            .json()
            .await
            .context("Failed to parse build_swap_ix response")?;

        let ix_data = response.into_result()?;
        self.convert_to_instruction(ix_data.program_id, ix_data.accounts, ix_data.data_base64)
    }

    /// Introspect a Raydium CLMM pool to get pool details
    pub async fn introspect_raydium(
        &self,
        pool: &str,
    ) -> Result<RaydiumIntrospectResponse> {
        let _permit = self.acquire_permit().await;
        
        let url = format!("{}/raydium/introspect", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "pool": pool }))
            .send()
            .await
            .context("Failed to send raydium introspect request")?;

        let response: SidecarResponse<RaydiumIntrospectResponse> = response
            .json()
            .await
            .context("Failed to parse raydium introspect response")?;

        response.into_result()
    }

    // ==================== Raydium CLMM Methods ====================

    /// Build a Raydium CLMM swap instruction
    pub async fn build_raydium_clmm_swap_ix(
        &self,
        pool: &str,
        amount: u64,
        other_amount_threshold: u64,
        is_base_input: bool,
        zero_for_one: bool,
        user: &Pubkey,
        input_token_account: &Pubkey,
        output_token_account: &Pubkey,
    ) -> Result<Instruction> {
        let request = RaydiumSwapIxRequest {
            pool: pool.to_string(),
            amount: amount.to_string(),
            other_amount_threshold: other_amount_threshold.to_string(),
            is_base_input,
            zero_for_one,
            user: user.to_string(),
            input_token_account: input_token_account.to_string(),
            output_token_account: output_token_account.to_string(),
        };

        let url = format!("{}/raydium/build_swap_ix", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send Raydium build_swap_ix request")?;

        let response: SidecarResponse<RaydiumSwapIxResponse> = response
            .json()
            .await
            .context("Failed to parse Raydium build_swap_ix response")?;

        let ix_data = response.into_result()?;
        self.convert_to_instruction(ix_data.program_id, ix_data.accounts, ix_data.data_base64)
    }

    // ==================== Pump.fun Methods ====================

    /// Get a quote for a Pump.fun swap
    pub async fn quote_pump(
        &self,
        pool: &str,
        side: Side,
        amount_in: u64,
        user: Option<&str>,
    ) -> Result<PumpQuoteResponse> {
        let request = PumpQuoteRequest {
            pool: pool.to_string(),
            side,
            amount_in: amount_in.to_string(),
            user: user.map(|s| s.to_string()),
        };

        let url = format!("{}/pump/quote", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send quote request")?;

        let response: SidecarResponse<PumpQuoteResponse> = response
            .json()
            .await
            .context("Failed to parse quote response")?;

        response.into_result()
    }

    // ==================== Slippage Quote Methods ====================

    /// Get slippage quote (buy/sell) for a pool
    pub async fn quote_slippage(
        &self,
        pool: &str,
        dex: &str,
        trade_size_quote: f64,
    ) -> Result<SlippageQuoteResponse> {
        let _permit = self.acquire_permit().await;

        let request = SlippageQuoteRequest {
            pool: pool.to_string(),
            dex: dex.to_string(),
            trade_size_quote,
        };

        let url = format!("{}/slippage", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send slippage quote request")?;

        let response: SidecarResponse<SlippageQuoteResponse> = response
            .json()
            .await
            .context("Failed to parse slippage quote response")?;

        response.into_result()
    }

    /// Build a Pump.fun swap instruction
    pub async fn build_pump_swap_ix(
        &self,
        pool: &str,
        side: Side,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        user_token_in: Option<&Pubkey>,
        user_token_out: Option<&Pubkey>,
    ) -> Result<Instruction> {
        let request = PumpSwapIxRequest {
            pool: pool.to_string(),
            side,
            amount_in: amount_in.to_string(),
            min_amount_out: min_amount_out.to_string(),
            user: user.to_string(),
            user_token_in: user_token_in.map(|p| p.to_string()),
            user_token_out: user_token_out.map(|p| p.to_string()),
        };

        let url = format!("{}/pump/build_swap_ix", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send build_swap_ix request")?;

        let response: SidecarResponse<PumpSwapIxResponse> = response
            .json()
            .await
            .context("Failed to parse build_swap_ix response")?;

        let ix_data = response.into_result()?;
        self.convert_to_instruction(ix_data.program_id, ix_data.accounts, ix_data.data_base64)
    }

    // ==================== Orca Whirlpool Methods ====================

    /// Build an Orca Whirlpool swap instruction
    pub async fn build_orca_whirlpool_swap_ix(
        &self,
        pool: &str,
        side: Side,
        amount_in: u64,
        min_amount_out: u64,
        user: &Pubkey,
        user_token_in: Option<&Pubkey>,
        user_token_out: Option<&Pubkey>,
    ) -> Result<Instruction> {
        let request = OrcaSwapIxRequest {
            pool: pool.to_string(),
            side,
            amount_in: amount_in.to_string(),
            min_amount_out: min_amount_out.to_string(),
            user: user.to_string(),
            user_token_in: user_token_in.map(|p| p.to_string()),
            user_token_out: user_token_out.map(|p| p.to_string()),
        };

        let url = format!("{}/orca/build_swap_ix", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send Orca build_swap_ix request")?;

        let response: SidecarResponse<OrcaSwapIxResponse> = response
            .json()
            .await
            .context("Failed to parse Orca build_swap_ix response")?;

        let ix_data = response.into_result()?;
        self.convert_to_instruction(ix_data.program_id, ix_data.accounts, ix_data.data_base64)
    }

    /// Introspect an Orca Whirlpool pool to get pool details
    pub async fn introspect_orca(
        &self,
        pool: &str,
    ) -> Result<OrcaIntrospectResponse> {
        let _permit = self.acquire_permit().await;
        
        let url = format!("{}/orca/introspect", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "pool": pool }))
            .send()
            .await
            .context("Failed to send orca introspect request")?;

        let response: SidecarResponse<OrcaIntrospectResponse> = response
            .json()
            .await
            .context("Failed to parse orca introspect response")?;

        response.into_result()
    }

    // ==================== Raydium AMM V4 Methods ====================

    /// Introspect a Raydium AMM V4 pool to get pool details
    pub async fn introspect_raydium_amm(
        &self,
        pool: &str,
    ) -> Result<RaydiumAmmIntrospectResponse> {
        let _permit = self.acquire_permit().await;
        
        let url = format!("{}/raydium_amm/introspect", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "pool": pool }))
            .send()
            .await
            .context("Failed to send raydium_amm introspect request")?;

        let response: SidecarResponse<RaydiumAmmIntrospectResponse> = response
            .json()
            .await
            .context("Failed to parse raydium_amm introspect response")?;

        response.into_result()
    }

    /// Build a Raydium AMM V4 swap instruction
    pub async fn build_raydium_amm_swap_ix(
        &self,
        pool: &str,
        amount_in: u64,
        min_amount_out: u64,
        side: &str, // "buy" or "sell"
        user: &Pubkey,
        user_token_in: &Pubkey,
        user_token_out: &Pubkey,
    ) -> Result<Instruction> {
        let request = RaydiumAmmSwapIxRequest {
            pool: pool.to_string(),
            amount_in: amount_in.to_string(),
            min_amount_out: min_amount_out.to_string(),
            side: side.to_string(),
            user: user.to_string(),
            user_token_in: user_token_in.to_string(),
            user_token_out: user_token_out.to_string(),
        };

        let url = format!("{}/raydium_amm/build_swap_ix", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send Raydium AMM build_swap_ix request")?;

        let response: SidecarResponse<RaydiumAmmSwapIxResponse> = response
            .json()
            .await
            .context("Failed to parse Raydium AMM build_swap_ix response")?;

        let ix_data = response.into_result()?;
        self.convert_to_instruction(ix_data.program_id, ix_data.accounts, ix_data.data_base64)
    }

    // ==================== Introspection Methods ====================

    /// Introspect a Meteora DLMM pool to get dependent accounts
    pub async fn introspect_meteora(
        &self,
        pool: &str,
    ) -> Result<MeteoraIntrospectResponse> {
        let _permit = self.acquire_permit().await;
        
        let url = format!("{}/meteora/introspect", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "pool": pool }))
            .send()
            .await
            .context("Failed to send meteora introspect request")?;

        let response: SidecarResponse<MeteoraIntrospectResponse> = response
            .json()
            .await
            .context("Failed to parse meteora introspect response")?;

        response.into_result()
    }

    /// Batch introspect multiple Meteora DLMM pools in one request
    /// This reduces RPC pressure by processing pools sequentially on the sidecar
    pub async fn batch_introspect_meteora(
        &self,
        pools: &[String],
    ) -> Result<MeteoraBatchIntrospectResponse> {
        let start = Instant::now();
        debug!(
            "Sidecar batch_introspect_meteora start (pools={})",
            pools.len()
        );
        let url = format!("{}/meteora/batch_introspect", self.base_url);
        let response = timeout(
            Duration::from_secs(20),
            self.client
                .post(&url)
                .json(&serde_json::json!({ "pools": pools }))
                .send(),
        )
        .await
        .map_err(|_| anyhow!("meteora batch_introspect timed out after 20s"))?
        .context("Failed to send meteora batch_introspect request")?;

        let response: SidecarResponse<MeteoraBatchIntrospectResponse> = response
            .json()
            .await
            .context("Failed to parse meteora batch_introspect response")?;

        let result = response.into_result();
        if result.is_err() {
            warn!(
                "Sidecar batch_introspect_meteora failed after {:?}",
                start.elapsed()
            );
        } else {
            debug!(
                "Sidecar batch_introspect_meteora done in {:?}",
                start.elapsed()
            );
        }
        result
    }

    /// Introspect a Pump.fun pool to get dependent accounts
    pub async fn introspect_pump(
        &self,
        pool: &str,
    ) -> Result<PumpIntrospectResponse> {
        let _permit = self.acquire_permit().await;
        
        let url = format!("{}/pump/introspect", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "pool": pool }))
            .send()
            .await
            .context("Failed to send pump introspect request")?;

        let response: SidecarResponse<PumpIntrospectResponse> = response
            .json()
            .await
            .context("Failed to parse pump introspect response")?;

        response.into_result()
    }

    // ==================== List Methods ====================

    /// List all Pump.fun tokens from the sidecar (DEPRECATED - returns empty)
    pub async fn list_pump_tokens(&self) -> Result<PumpListTokensResponse> {
        let url = format!("{}/pump/list_tokens", self.base_url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send list_pump_tokens request")?;

        // Check if response indicates an error
        let status = response.status();
        let text = response.text().await.context("Failed to read response body")?;

        // Try to parse as success response first
        if let Ok(resp) = serde_json::from_str::<PumpListTokensResponse>(&text) {
            if resp.ok {
                return Ok(resp);
            }
        }

        // Try to parse as error response
        if let Ok(err_resp) = serde_json::from_str::<ListErrorResponse>(&text) {
            anyhow::bail!("Sidecar error: {}", err_resp.message);
        }

        anyhow::bail!(
            "Failed to parse list_pump_tokens response (status {}): {}",
            status,
            text.chars().take(200).collect::<String>()
        )
    }

    /// List hot meme token arbitrage pools from DexScreener
    /// Returns 2 pools per token (on different DEXes) for arbitrage
    pub async fn list_dexscreener_pools(&self) -> Result<MeteoraListPairsResponse> {
        let url = format!("{}/dexscreener/hot_pools", self.base_url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send list_dexscreener_pools request")?;

        let status = response.status();
        let text = response.text().await.context("Failed to read response body")?;

        // Try to parse as success response
        if let Ok(resp) = serde_json::from_str::<MeteoraListPairsResponse>(&text) {
            if resp.ok {
                return Ok(resp);
            }
        }

        // Try to parse as error response
        if let Ok(err_resp) = serde_json::from_str::<ListErrorResponse>(&text) {
            anyhow::bail!("Sidecar error: {}", err_resp.message);
        }

        anyhow::bail!(
            "Failed to parse list_dexscreener_pools response (status {}): {}",
            status,
            text.chars().take(200).collect::<String>()
        )
    }

    // ==================== Helper Methods ====================

    /// Convert sidecar response into a Solana Instruction
    fn convert_to_instruction(
        &self,
        program_id_str: String,
        accounts: Vec<AccountMeta>,
        data_base64: String,
    ) -> Result<Instruction> {
        // Parse program ID
        let program_id = Pubkey::from_str(&program_id_str)
            .context("Invalid program ID")?;

        // Convert accounts
        let solana_accounts: Result<Vec<SolanaAccountMeta>> = accounts
            .into_iter()
            .map(|acc| {
                let pubkey = Pubkey::from_str(&acc.pubkey)
                    .context(format!("Invalid pubkey: {}", acc.pubkey))?;
                Ok(SolanaAccountMeta {
                    pubkey,
                    is_signer: acc.is_signer,
                    is_writable: acc.is_writable,
                })
            })
            .collect();

        let solana_accounts = solana_accounts?;

        // Decode base64 data
        let data = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            data_base64,
        )
        .context("Failed to decode base64 instruction data")?;

        Ok(Instruction {
            program_id,
            accounts: solana_accounts,
            data,
        })
    }
}
