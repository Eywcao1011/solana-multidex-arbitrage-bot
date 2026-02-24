# Pool Manager

Dynamic pool discovery and subscription management for cross-DEX SOL pairs.

## Overview

The PoolManager periodically fetches token/pool data from external APIs (Meteora + DexScreener), builds a filtered runtime pool set, and updates subscriptions automatically.

## Architecture

```
┌─────────────────┐     HTTP      ┌──────────────────────┐
│  Rust Monitor   │──────────────▶│ Meteora + DexScreener│
│  (PoolManager)  │◀──────────────│ (token/pool sources) │
└────────┬────────┘   JSON        └──────────────────────┘
         │
         │ Filters + Diff
         ▼
┌─────────────────┐
│  Subscriptions  │
│  (WS / gRPC)    │
└─────────────────┘
```

## Configuration

All settings are configured via environment variables (see `.env.example`):

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `SIDECAR_URL` | Sidecar HTTP endpoint | `http://127.0.0.1:8080` |
| `POOL_REFRESH_SECS` | Refresh interval in seconds | `300` (5 min) |

### General Filters

| Variable | Description | Default |
|----------|-------------|---------|
| `POOL_QUOTE_WHITELIST` | Comma-separated quote mint addresses | SOL, USDC, USDT |
| `MIN_TVL_USD` | Minimum TVL in USD | `0` |
| `MIN_VOL24H_USD` | Minimum 24h volume in USD | `0` |

### Pump.fun Filters

| Variable | Description | Default |
|----------|-------------|---------|
| `PUMP_ALLOW_BONDING` | Include bonding curve tokens | `false` |
| `PUMP_MIN_LIQ_USD` | Minimum liquidity in USD | `0` |
| `PUMP_MIN_MCAP_USD` | Minimum market cap in USD | `0` |

### White/Black Lists

| Variable | Description | Default |
|----------|-------------|---------|
| `POOL_WHITELIST` | Always include these pools | (empty) |
| `POOL_BLACKLIST` | Always exclude these pools | (empty) |

## Filter Priority

1. **Blacklist** - Pools in blacklist are always excluded
2. **Whitelist** - Pools in whitelist bypass all other filters
3. **Quote Whitelist** - Pool must have quote/base in whitelist
4. **Numeric Filters** - TVL, volume, liquidity, market cap thresholds

## Usage

### With Dynamic Pool Discovery

```rust
use arb::monitor::run_monitor_with_pool_manager;

// Start monitor with dynamic pool management
run_monitor_with_pool_manager(
    ctx,
    ws_url,
    static_pools,  // Optional static pools always monitored
    thresholds,
    commitment,
).await?;
```

### Standalone PoolManager

```rust
use arb::pool_manager::PoolManager;

// Create manager
let manager = PoolManager::from_env()?;

// Manual refresh
let result = manager.refresh().await?;
println!("Found {} pools after filtering", result.filtered);

// Get current pools
let pools = manager.get_pools().await;

// Convert to descriptors for subscription
let descriptors = manager.to_descriptors().await;
```

## Logs

The PoolManager logs refresh statistics at each cycle:

```
[PoolManager] Refresh: candidates=5000, filtered=150, +3 -1
```

- **candidates**: Total pools fetched from sidecar
- **filtered**: Pools remaining after filters
- **+N**: Newly added pools
- **-N**: Removed pools

## Error Handling

- **Sidecar unavailable**: Keeps existing pool set, logs warning
- **Parse errors**: Skips individual records, logs at debug level
- **Refresh failures**: Automatic retry with backoff, never crashes

## Data Flow

1. **Fetch token universe**: query Meteora API for SOL pairs
2. **Expand pools**: query DexScreener per token to gather cross-DEX pools
3. **Parse**: convert API responses to `PoolMeta`
4. **Filter**: apply quote/liquidity/volume/allowlist/denylist rules
5. **Pair Intersection** (optional): keep only pairs existing on all required DEXes
6. **Diff**: compare against current runtime pool set
7. **Update + Subscribe**: add/remove pools and update subscriptions

## Cross-DEX Pair Intersection

When `PAIR_INTERSECTION_ONLY=true`, only trading pairs that exist on **all specified DEXes** are kept.

### Configuration

```bash
# Enable pair intersection filter
PAIR_INTERSECTION_ONLY=true

# Specify which DEXes must have the pair (default: meteora_dlmm,pump_fun_dlmm)
PAIR_INTERSECTION_DEXES=meteora_dlmm,pump_fun_dlmm
```

### Supported DEX Values

| Value | DEX |
|-------|-----|
| `meteora_dlmm` | Meteora DLMM |
| `pump_fun_dlmm` | Pump.fun |
| `orca_whirlpool` | Orca Whirlpool |
| `raydium_clmm` | Raydium CLMM |
| `meteora_lb` | Meteora LB |

### Example

With default settings (`meteora_dlmm,pump_fun_dlmm`):

| Pair | Meteora | Pump.fun | Result |
|------|---------|----------|--------|
| MEME/SOL | ✅ | ✅ | ✅ Kept (both pools) |
| SOL/USDC | ✅ | ❌ | ❌ Dropped |
| DOGE/SOL | ❌ | ✅ | ❌ Dropped |

### Logs

When enabled, logs show intersection statistics:
```
[PoolManager] Pair intersection: kept 25 pairs (50 pools), dropped 100 pairs
```

## Pump.fun Discovery Notes

Pump-related pools are discovered from DexScreener results while scanning SOL pairs.
Optional Moralis integration can improve token coverage:

```bash
MORALIS_API_KEY=your_moralis_api_key
```

Without Moralis, PoolManager still works and continues discovering pools from Meteora + DexScreener paths.

### Subscription Target

Pump.fun tokens are subscribed using the **bonding curve PDA**, not the mint address:

- The sidecar derives the bonding curve from each mint: `seeds = ['bonding-curve', mint]`
- If sidecar provides `bonding_curve`, PoolManager uses it directly
- If missing, PoolManager derives it locally using the same PDA logic
- If derivation fails, the token is filtered out (not subscribed)

This ensures `pump_fun::decode` receives the correct account data (bonding curve state, not mint metadata).

## Refresh Behavior

The PoolManager handles source failures gracefully:

| Meteora | Pump | Behavior |
|---------|------|----------|
| ✅ OK | ✅ OK | Update pool set normally |
| ✅ OK | ❌ Fail | Update with Meteora only, log warning |
| ❌ Fail | ✅ OK | Update with Pump only, log warning |
| ❌ Fail | ❌ Fail | Keep existing pools, return error |

Deduplication by pool key prevents duplicate subscriptions if the same pool appears in both sources.
