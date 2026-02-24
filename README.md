# Arb (Spot-Only)

Solana multi-DEX arbitrage bot for spot-only atomic round trips:
`quote -> base -> quote` in one transaction.

Supported DEX types:
- Meteora DLMM / LB / DAMM
- Orca Whirlpool
- Raydium CLMM / CPMM / AMM V4
- Pump.fun DLMM

## Execution Flow (Runtime Order)
1. **Bootstrap**: load `.env`, start logger/watchdog/runtime, create RPC context and caches. If `EXPORT_POOLS_ONLY=true`, the process refreshes pools, writes `filtered_pools.txt`, and exits. If `ENABLE_MONITOR=false`, it exits after init. If `SKIP_WSOL_WRAP=true`, startup verifies signer wSOL ATA existence/balance.

2. **Resolve trading mode**: parse thresholds and sizing (`ARBITRAGE_ALERT_BPS`, trade size/range, slippage buffer) and lock run mode using `ENABLE_TRADE_EXECUTION` + `ARBITRAGE_DRY_RUN_ONLY` (`monitor-only` / `simulate` / `live`).

3. **Load static pools (optional)**: parse forced pools from env (`ORCA_POOLS`, `RAYDIUM_POOLS`, `METEORA_*_POOLS`, `PUMP_FUN_POOLS`). These are always monitored in addition to dynamic pools.

4. **Discover dynamic pools**: PoolManager queries Meteora API for SOL-related tokens, then queries DexScreener per token for cross-DEX pools. Initial gating keeps Solana/wSOL pairs and applies liquidity+volume thresholds (`DEXSCREENER_MIN_LIQUIDITY`, `MIN_VOL24H_USD`).

5. **Normalize + filter**: convert raw pool records to internal `PoolMeta`, classify DEX kind (including owner-based Meteora classification via RPC), then apply user rules (`POOL_WHITELIST`, `POOL_BLACKLIST`, `POOL_QUOTE_WHITELIST`, `PAIR_INTERSECTION_ONLY`, `PAIR_INTERSECTION_DEXES`, `MAX_DYNAMIC_POOLS`). Output is the active runtime pool set.

6. **Warmup/cache priming**: prefetch DEX-specific static state (tick/bin arrays, vault/static metadata), build first snapshots, and seed caches so hot-path opportunity checks and tx building avoid extra RPC round trips.

7. **Subscribe state stream**: start WS or gRPC account subscriptions based on `SUBSCRIPTION_MODE` (`ws` via websocket RPC, `grpc` via Yellowstone using `GRPC_ENDPOINT` and optional `GRPC_X_TOKEN`). Continuously update snapshots from streamed account changes.

8. **Calibrate slippage**: buy-leg slippage comes from on-chain simulation path; sell-leg slippage comes from sidecar `/slippage`. Results are cached per pool and refreshed by `SLIPPAGE_REFRESH_INTERVAL_MINS`; these values are mandatory opportunity gates.

9. **Detect opportunities**: on every relevant snapshot update, compute spread and require it to beat full threshold cost: `buy_slippage + sell_slippage + SLIPPAGE_BUFFER_PCT`. Passing candidates enter strategy/dispatcher checks.

10. **Prepare accounts + build atomic tx**: resolve ATA mapping (`TRADE_ACCOUNTS` override supported), optionally manage ALT (`ALT_AUTO_EXTEND`, `JITO_LOOKUP_TABLES`), then build one atomic v0 route: fixed quote-in buy (`ARBITRAGE_TRADE_SIZE_QUOTE`) and protected sell leg back to quote.

11. **Execute or simulate**: in `monitor-only` no submission occurs; in `simulate` mode tx is built/simulated only; in `live` mode tx is sent through configured relay path (`JITO_GRPC_URL` or `JITO_RELAY_URL`, plus optional tip settings `JITO_*`). Loop continues with cooldown/risk guards and new state updates.

## Important Runtime Mechanisms (Easy to Miss)
- **PoolManager is periodic, not one-shot**: after startup it refreshes by `POOL_REFRESH_SECS`. Current implementation performs a full refresh cycle (rebuild pool set, cancel old pool tasks, resubscribe filtered pools, refresh runtime caches).
- **Pair-level survivability filter**: dynamic discovery keeps only pairs that still have at least 2 tradable pools after liquidity/volume filtering, so single-pool pairs are dropped.
- **Quarantine + runtime blacklist**: if a pool repeatedly fails decode/update, it is quarantined for a cooldown window; specific hard failures can also push it into runtime blacklist so next refresh excludes it.
- **Opportunity requires slippage data**: if buy/sell slippage cache is missing, the opportunity is skipped instead of being traded.
- **Dispatcher guards are strict**: in-flight dedupe, pair cooldown (`PAIR_COOLDOWN_SECS`), and pool-slot cooldown prevent repeated firing on the same path.
- **Strategy filter is a second gate**: `STRATEGY_*` can reject by net profit, spread, whitelist/blacklist, active jobs, or strategy cooldown.
- **Dry-run has two behaviors**: normal dry-run (`ARBITRAGE_DRY_RUN_ONLY=true`) keeps full monitor pipeline but replaces send with simulate; optional `DRY_RUN_SWEEP=true` runs one warmed sweep and exits.
- **Live submission path expects relay config**: transaction submission currently depends on Jito client config (`JITO_GRPC_URL` or `JITO_RELAY_URL`). Missing relay config causes submission failure.
- **Sidecar is not only for slippage**: it is also used for DEX introspection and instruction-building paths needed by multiple pools/executors.

## Disclaimer
This repository is for research and engineering demonstration. Use at your own risk.
No financial advice.

## Requirements
- Linux/macOS
- Rust stable (latest recommended)
- Node.js 20 (required by sidecar dependencies)
- `npm`
- Solana mainnet RPC + WS endpoints

## Quick Start
1. Install dependencies:
```bash
npm ci --prefix sidecar
cargo build --release
```

2. Create env file:
```bash
cp .env.example .env
```

3. Fill required fields in `.env`:
- `SOLANA_HTTP_URL`
- `SOLANA_WS_URL`
- `RPC_URL` (sidecar RPC, usually same as `SOLANA_HTTP_URL`)
- If `SUBSCRIPTION_MODE=grpc`: `GRPC_ENDPOINT`
- If live trading: `SOLANA_SIGNER_KEYPAIR`

4. Run bot + sidecar together:
```bash
./run_with_sidecar.sh
```

## Run Modes
- Monitor only (safe default):
```env
ENABLE_TRADE_EXECUTION=false
ARBITRAGE_DRY_RUN_ONLY=true
```

- Simulate execution (build execution flow but do not send):
```env
ENABLE_TRADE_EXECUTION=true
ARBITRAGE_DRY_RUN_ONLY=true
```

- Live execution:
```env
ENABLE_TRADE_EXECUTION=true
ARBITRAGE_DRY_RUN_ONLY=false
```
Also requires:
- `SOLANA_SIGNER_KEYPAIR`
- Funded wallet + funded wSOL ATA (if `SKIP_WSOL_WRAP=true`)

- Export pool set and exit:
```env
EXPORT_POOLS_ONLY=true
```

## Key Env Variables
See [`.env.example`](.env.example) for full list.

High-impact settings:
- `ARBITRAGE_TRADE_SIZE_QUOTE`: per-trade quote size
- `SLIPPAGE_BUFFER_PCT`: opportunity decision buffer
- `MIN_TVL_USD`, `MIN_VOL24H_USD`: dynamic pool filters
- `DEXSCREENER_MIN_LIQUIDITY`, `DEXSCREENER_DELAY_MS`: discovery controls
- `POOL_FILTER_TRADE_SIZE_QUOTE`, `POOL_FILTER_MAX_SLIPPAGE_BPS`: slippage gating
- `ALT_AUTO_EXTEND`, `JITO_*`: transaction routing and priority setup

## Sidecar
Sidecar serves instruction building and introspection endpoints used by Rust runtime.

Entry file:
- [`sidecar/src/index.ts`](sidecar/src/index.ts)

The launcher script `./run_with_sidecar.sh`:
- Loads `.env`
- Ensures Node.js 20
- Starts sidecar
- Waits for `/health`
- Starts `cargo run --release`

## Observability
- Application logs: `logs/`
- Watchdog log: `logs/arb_watchdog.log`
- Sidecar log (launcher): `sidecar.log`
- Pool export (when enabled): `filtered_pools.txt`

## Troubleshooting
- `GRPC_ENDPOINT is required when SUBSCRIPTION_MODE=grpc`
  - Set `GRPC_ENDPOINT`, or switch to `SUBSCRIPTION_MODE=ws`.

- `Node.js v20 not found`
  - Install with `nvm install 20`.

- `SKIP_WSOL_WRAP=true but wSOL ATA doesn't exist`
  - Create/fund wSOL ATA, or set `SKIP_WSOL_WRAP=false`.

- Sidecar starts but quotes/introspect fail
  - Verify `RPC_URL` is valid and has enough rate limit.

## Development Checks
Rust:
```bash
cargo check --locked
```

Sidecar types:
```bash
cd sidecar
npx tsc --noEmit
```

## CI
GitHub Actions CI is included at:
- [`.github/workflows/ci.yml`](.github/workflows/ci.yml)

It runs:
- `cargo check --locked`
- `npm ci --prefix sidecar`
- `npx tsc --noEmit`

## GitHub Upload Checklist
- Do not commit `.env`, keypairs, or any private credentials.
- Ensure large local build folders are not committed (`target/`, `sidecar/node_modules/`).
- Run checks before push:
```bash
cargo check --locked
npm ci --prefix sidecar
npx --prefix sidecar tsc --noEmit
```
- If this directory is not a git repo yet, initialize and push:
```bash
git init
git add .
git commit -m "Initial public release"
git branch -M main
git remote add origin <your_repo_url>
git push -u origin main
```
