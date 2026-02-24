#!/usr/bin/env bash
set -e

# Load .env into shell env (for EXPORT_POOLS_ONLY etc.)
if [ -f ".env" ]; then
    # shellcheck disable=SC2046
    set -a
    . ".env"
    set +a
fi

# ============================================================
# Run Arb with Sidecar
# ============================================================
# The Rust bot now uses PoolManager by default:
# - Dynamically discovers pools (Meteora + DexScreener path)
# - No manual pool configuration (ORCA_POOLS, etc.) required
# - Applies filters from .env (POOL_QUOTE_WHITELIST, MIN_TVL_USD, etc.)
# - Periodically refreshes and subscribes to new pools
#
# Static pools in .env are still supported for forced subscriptions.
# ============================================================

# Default sidecar port / URL
export SIDECAR_PORT=${SIDECAR_PORT:-8080}
export SIDECAR_URL=${SIDECAR_URL:-http://127.0.0.1:${SIDECAR_PORT}}

# Kill any stale process already listening on the sidecar port (prevents EADDRINUSE)
if command -v lsof >/dev/null 2>&1; then
    STALE_PIDS=$(lsof -t -i :"${SIDECAR_PORT}" || true)
    if [ -n "${STALE_PIDS}" ]; then
        echo "Found process on port ${SIDECAR_PORT}: ${STALE_PIDS}; killing..."
        kill ${STALE_PIDS} 2>/dev/null || true
        sleep 1
    fi
fi

# Load nvm and use Node.js v20 (required for @meteora-ag/dlmm compatibility)
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
nvm use 20 >/dev/null 2>&1 || {
    echo "Error: Node.js v20 not found. Install with: nvm install 20"
    exit 1
}

# Start sidecar in background, log to sidecar.log
cd sidecar
npm run dev > ../sidecar.log 2>&1 &
SIDECAR_PID=$!
cd ..
echo "Sidecar starting (PID=${SIDECAR_PID})..."

# Wait for sidecar to be ready (max 30 seconds)
MAX_WAIT=30
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if curl -s "${SIDECAR_URL}/health" > /dev/null 2>&1; then
        echo "Sidecar ready at ${SIDECAR_URL}"
        break
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo "Error: Sidecar failed to start within ${MAX_WAIT}s. Check sidecar.log"
    kill $SIDECAR_PID 2>/dev/null || true
    exit 1
fi

# Ensure sidecar is terminated on exit
cleanup() {
    echo "Stopping sidecar..."
    kill $SIDECAR_PID 2>/dev/null || true
    wait $SIDECAR_PID 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start Rust application (release mode for performance)
# Disable LTO here to avoid LLVM bitcode load failures on some toolchains.
export CARGO_PROFILE_RELEASE_LTO=${CARGO_PROFILE_RELEASE_LTO:-false}
cargo run --release
