use anyhow::{anyhow, Result};
use solana_instruction::Instruction;
use solana_pubkey::Pubkey;

use crate::dex::{DexKind, PoolSnapshot, TradeSide};

pub mod accounts;
pub mod arbitrage;
pub mod dispatcher;
pub mod execution;
pub mod meteora;
pub mod meteora_damm;
pub mod meteora_dlmm;
pub mod orca;
pub mod pump_fun;
pub mod raydium;
pub mod raydium_amm;
pub mod raydium_cpmm;
pub mod strategy;
pub mod tx;

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct TradeAccounts {
    pub payer: Pubkey,
    pub user_base_account: Pubkey,
    pub user_quote_account: Pubkey,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TradeRequest {
    pub side: TradeSide,
    /// For exact-out: the base amount to buy/sell
    /// For exact-in Buy: ignored (use quote_amount_in instead)
    /// For exact-in Sell: the base amount to sell
    pub base_amount: f64,
    pub max_slippage_bps: f64,
    /// For Buy leg (exact-in): this is min_base_out (至少买到多少 base)
    /// For Sell leg: this is min_quote_out (至少拿回多少 quote)
    pub min_amount_out: Option<f64>,
    /// For Buy leg (exact-in): fixed quote amount to spend (固定花费多少 quote)
    /// For Sell leg: not used
    pub quote_amount_in: Option<f64>,
    pub accounts: TradeAccounts,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TradePlan {
    pub instructions: Vec<Instruction>,
    pub description: String,
}

impl TradePlan {
    #[allow(dead_code)]
    pub fn empty(msg: impl Into<String>) -> Self {
        Self {
            instructions: Vec::new(),
            description: msg.into(),
        }
    }
}

#[allow(dead_code)]
pub trait TradeExecutor: Send + Sync {
    fn dex(&self) -> DexKind;
    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan>;
}

#[allow(dead_code)]
pub fn executor_for(kind: DexKind) -> Box<dyn TradeExecutor> {
    match kind {
        DexKind::PumpFunDlmm => Box::new(pump_fun::PumpFunExecutor::default()),
        DexKind::RaydiumClmm => Box::new(raydium::RaydiumExecutor::default()),
        DexKind::MeteoraDlmm => Box::new(meteora_dlmm::MeteoraDlmmExecutor::default()),
        DexKind::MeteoraLb => Box::new(meteora::MeteoraLbExecutor::default()),
        DexKind::OrcaWhirlpool => Box::new(orca::OrcaExecutor::default()),
        DexKind::MeteoraDammV1 | DexKind::MeteoraDammV2 => {
            Box::new(meteora_damm::MeteoraDammExecutor::default())
        }
        DexKind::RaydiumAmmV4 => Box::new(raydium_amm::RaydiumAmmExecutor::default()),
        DexKind::RaydiumCpmm => Box::new(raydium_cpmm::RaydiumCpmmExecutor::default()),
    }
}

#[allow(dead_code)]
struct UnsupportedExecutor {
    kind: DexKind,
}

impl TradeExecutor for UnsupportedExecutor {
    fn dex(&self) -> DexKind {
        self.kind
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, _request: &TradeRequest) -> Result<TradePlan> {
        Err(anyhow!(
            "trade execution not yet implemented for {:?}",
            snapshot.descriptor.kind
        ))
    }
}
