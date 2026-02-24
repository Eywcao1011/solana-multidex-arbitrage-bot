//! Pool Manager Module
//!
//! Periodically fetches pool/token lists from sidecar and maintains
//! a filtered set of pools to monitor.

mod config;
mod manager;
mod types;

pub use config::PoolFilterConfig;
pub use manager::{PoolManager, RefreshResult};
pub use types::PoolMeta;
