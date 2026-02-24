use serde::{Deserialize, Serialize};

/// Side of the swap: buying base or buying quote
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    InBase,
    InQuote,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::InBase => write!(f, "in_base"),
            Side::InQuote => write!(f, "in_quote"),
        }
    }
}

/// Account metadata for instructions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountMeta {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
}

/// Generic response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SidecarResponse<T> {
    Success(T),
    Error { ok: bool, error: String },
}

impl<T> SidecarResponse<T> {
    pub fn into_result(self) -> anyhow::Result<T> {
        match self {
            SidecarResponse::Success(data) => Ok(data),
            SidecarResponse::Error { error, .. } => {
                anyhow::bail!("Sidecar error: {}", error)
            }
        }
    }
}

// ==================== Meteora DLMM Types ====================

/// Request for Meteora DLMM quote
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraQuoteRequest {
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Response from Meteora DLMM quote endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraQuoteResponse {
    pub ok: bool,
    pub dex: String,
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    pub amount_out: String,
    pub price_impact_pct: String,
    pub fee: String,
}

/// Request for Meteora DLMM swap instruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraSwapIxRequest {
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    pub min_amount_out: String,
    pub user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_token_in: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_token_out: Option<String>,
}

/// Response from Meteora DLMM build_swap_ix endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraSwapIxResponse {
    pub ok: bool,
    pub dex: String,
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data_base64: String,
}

/// Response from Raydium CLMM introspect endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumIntrospectResponse {
    pub ok: bool,
    pub dex: Option<String>,
    pub pool: Option<String>,
    pub program_id: Option<String>,
    pub token_mint_0: Option<String>,
    pub token_mint_1: Option<String>,
    pub vault_0: Option<String>,
    pub vault_1: Option<String>,
    pub decimals_0: Option<u8>,
    pub decimals_1: Option<u8>,
    pub reserve_0: Option<String>,
    pub reserve_1: Option<String>,
    pub sqrt_price_x64: Option<String>,
    pub price: Option<String>,
    pub liquidity: Option<String>,
    pub tick_spacing: Option<i32>,
    pub tick_current_index: Option<i32>,
    pub fee_rate: Option<u64>,
    pub protocol_fee_rate: Option<u64>,
    pub fund_fee_rate: Option<u64>,
    pub slot: Option<u64>,
    pub error: Option<String>,
}

/// Response from Meteora DLMM introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraIntrospectResponse {
    pub ok: bool,
    pub dex: Option<String>,
    pub pool: Option<String>,
    pub program_id: Option<String>,
    pub token_mint_x: Option<String>,
    pub token_mint_y: Option<String>,
    pub reserve_x: Option<String>,
    pub reserve_y: Option<String>,
    pub decimals_x: Option<u8>,
    pub decimals_y: Option<u8>,
    pub reserve_x_amount: Option<String>,
    pub reserve_y_amount: Option<String>,
    pub bin_step: Option<u64>,
    pub active_id: Option<i32>,
    pub price: Option<String>,
    pub slot: Option<u64>,
    pub dependent_accounts: Option<Vec<String>>,
    pub bin_arrays: Option<Vec<String>>,  // deprecated, merged list
    pub bin_arrays_x_to_y: Option<Vec<String>>,  // Sell direction (X→Y)
    pub bin_arrays_y_to_x: Option<Vec<String>>,  // Buy direction (Y→X)
    pub warning: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraBatchIntrospectResult {
    pub pool: String,
    pub ok: bool,
    pub active_id: Option<i32>,
    pub bin_arrays: Option<Vec<String>>,  // deprecated, merged list
    pub bin_arrays_x_to_y: Option<Vec<String>>,  // Sell direction
    pub bin_arrays_y_to_x: Option<Vec<String>>,  // Buy direction
    pub error: Option<String>,
}

/// Response from Meteora DLMM batch introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraBatchIntrospectResponse {
    pub ok: bool,
    pub count: Option<usize>,
    pub success_count: Option<usize>,
    pub results: Option<Vec<MeteoraBatchIntrospectResult>>,
    pub error: Option<String>,
}

// ==================== Pump.fun Types ====================

/// Request for Pump.fun quote
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpQuoteRequest {
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Response from Pump.fun quote endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpQuoteResponse {
    pub ok: bool,
    pub dex: String,
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    pub amount_out: String,
    pub price_impact_pct: String,
    pub fee: String,
}

/// Request for Pump.fun swap instruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpSwapIxRequest {
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    pub min_amount_out: String,
    pub user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_token_in: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_token_out: Option<String>,
}

/// Response from Pump.fun build_swap_ix endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpSwapIxResponse {
    pub ok: bool,
    pub dex: String,
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data_base64: String,
}

// ==================== Slippage Quote Types ====================

/// Request for pool slippage quote
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlippageQuoteRequest {
    pub pool: String,
    pub dex: String,
    pub trade_size_quote: f64,
}

/// Response from slippage quote endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlippageQuoteResponse {
    pub ok: bool,
    pub dex: String,
    pub pool: String,
    pub trade_size_quote: f64,
    pub buy_slippage_pct: f64,
    pub sell_slippage_pct: f64,
    #[serde(default)]
    pub buy_fee_pct: Option<f64>,
    #[serde(default)]
    pub sell_fee_pct: Option<f64>,
    #[serde(default)]
    pub note: Option<String>,
}

// ==================== Orca Whirlpool Types ====================

/// Request for Orca Whirlpool swap instruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrcaSwapIxRequest {
    pub pool: String,
    pub side: Side,
    pub amount_in: String,
    pub min_amount_out: String,
    pub user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_token_in: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_token_out: Option<String>,
}

/// Response from Orca Whirlpool build_swap_ix endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrcaSwapIxResponse {
    pub ok: bool,
    pub dex: String,
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data_base64: String,
}

/// Request for Orca Whirlpool introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrcaIntrospectRequest {
    pub pool: String,
}

/// Response from Orca Whirlpool introspect endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrcaIntrospectResponse {
    pub ok: bool,
    pub dex: Option<String>,
    pub pool: Option<String>,
    pub program_id: Option<String>,
    pub token_mint_a: Option<String>,
    pub token_mint_b: Option<String>,
    pub token_vault_a: Option<String>,
    pub token_vault_b: Option<String>,
    pub decimals_a: Option<u8>,
    pub decimals_b: Option<u8>,
    pub reserve_a: Option<String>,
    pub reserve_b: Option<String>,
    pub sqrt_price_x64: Option<String>,
    pub price: Option<String>,
    pub liquidity: Option<String>,
    pub tick_spacing: Option<i32>,
    pub tick_current_index: Option<i32>,
    pub fee_rate: Option<u16>,
    pub protocol_fee_rate: Option<u16>,
    pub slot: Option<u64>,
    pub error: Option<String>,
}

// ==================== Raydium CLMM Types ====================

/// Request for Raydium CLMM swap instruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumSwapIxRequest {
    pub pool: String,
    pub amount: String,
    pub other_amount_threshold: String,
    pub is_base_input: bool,
    pub zero_for_one: bool,
    pub user: String,
    pub input_token_account: String,
    pub output_token_account: String,
}

/// Response from Raydium CLMM build_swap_ix endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumSwapIxResponse {
    pub ok: bool,
    pub dex: String,
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data_base64: String,
}

// ==================== Raydium AMM V4 Types ====================

/// Request for Raydium AMM V4 introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumAmmIntrospectRequest {
    pub pool: String,
}

/// Response from Raydium AMM V4 introspect endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumAmmIntrospectResponse {
    pub ok: bool,
    pub dex: Option<String>,
    pub pool: Option<String>,
    pub program_id: Option<String>,
    pub base_mint: Option<String>,
    pub quote_mint: Option<String>,
    pub base_vault: Option<String>,
    pub quote_vault: Option<String>,
    pub lp_mint: Option<String>,
    pub open_orders: Option<String>,
    pub target_orders: Option<String>,
    pub market_id: Option<String>,
    pub market_program_id: Option<String>,
    // Serum market accounts (read from market account data)
    pub serum_bids: Option<String>,
    pub serum_asks: Option<String>,
    pub serum_event_queue: Option<String>,
    pub serum_coin_vault: Option<String>,
    pub serum_pc_vault: Option<String>,
    pub serum_vault_signer: Option<String>,
    pub base_decimals: Option<u8>,
    pub quote_decimals: Option<u8>,
    pub base_reserve: Option<String>,
    pub quote_reserve: Option<String>,
    pub price: Option<String>,
    pub fee_rate: Option<f64>,
    pub trade_fee_numerator: Option<u64>,
    pub trade_fee_denominator: Option<u64>,
    pub swap_fee_numerator: Option<u64>,
    pub swap_fee_denominator: Option<u64>,
    pub status: Option<u64>,
    pub slot: Option<u64>,
    pub error: Option<String>,
}

/// Request for Raydium AMM V4 swap instruction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumAmmSwapIxRequest {
    pub pool: String,
    pub amount_in: String,
    pub min_amount_out: String,
    pub side: String, // "buy" or "sell"
    pub user: String,
    pub user_token_in: String,
    pub user_token_out: String,
}

/// Response from Raydium AMM V4 build_swap_ix endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaydiumAmmSwapIxResponse {
    pub ok: bool,
    pub dex: String,
    pub program_id: String,
    pub accounts: Vec<AccountMeta>,
    pub data_base64: String,
}

/// Response from Pump.fun introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpIntrospectResponse {
    pub ok: bool,
    pub dex: Option<String>,
    pub program_id: Option<String>,
    pub pool: Option<String>,
    pub mint: Option<String>,
    pub bonding_curve: Option<String>,
    pub associated_bonding_curve: Option<String>,
    pub fee_recipient: Option<String>,
    pub global: Option<String>,
    pub event_authority: Option<String>,
    pub slot: Option<u64>,
    pub dependent_accounts: Option<Vec<String>>,
    pub warning: Option<String>,
    pub error: Option<String>,
}

// ==================== Pool/Token Listing Types ====================

/// Meteora DLMM pair information from list endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraPairInfo {
    pub pair_address: String,
    pub base_mint: String,
    pub quote_mint: String,
    pub tvl_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub label: Option<String>,
    /// DEX identifier (e.g., "meteora", "raydium", "orca", "pumpswap")
    pub dex_id: Option<String>,
}

/// Response from /meteora/list_pairs endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeteoraListPairsResponse {
    pub ok: bool,
    pub pairs: Vec<MeteoraPairInfo>,
}

/// Pump.fun bonding state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PumpBondingState {
    Bonding,
    Graduated,
    Unknown,
}

/// Pump.fun token information from list endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpTokenInfo {
    pub mint: String,
    /// Bonding curve PDA address (derived from mint by sidecar)
    pub bonding_curve: Option<String>,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub liquidity_usd: Option<f64>,
    pub marketcap_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub bonding_state: Option<PumpBondingState>,
}

/// Response from /pump/list_tokens endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PumpListTokensResponse {
    pub ok: bool,
    pub tokens: Vec<PumpTokenInfo>,
}

/// Error response for list endpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListErrorResponse {
    pub ok: bool,
    pub message: String,
}
