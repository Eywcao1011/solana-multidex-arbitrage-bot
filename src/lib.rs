// Library exports for the Arb arbitrage bot

pub mod cache;
pub mod alt_manager;
pub mod calibration;
pub mod config;
pub mod constants;
pub mod dex;
pub mod grpc_subscription;
pub mod jito_grpc;
pub mod monitor;
pub mod log_utils;
pub mod pool_manager;
pub mod rpc;
pub mod sidecar_client;
pub mod subscription;
pub mod trade;

// Re-export commonly used types
pub use dex::{
    DexKind, NormalizedPair, Opportunity, PoolDescriptor, PoolSnapshot, PriceThresholds,
    TradeEstimates, TradeSide,
};
pub use rpc::AppContext;
