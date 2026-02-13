#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Shinzo Indexer Integration Test
#
# Usage:
#   ./scripts/shinzo-test.sh              # Start defra + indexer (HTTP mode)
#   RUST_FFI=1 ./scripts/shinzo-test.sh   # Start indexer with embedded Rust FFI
#   ./scripts/shinzo-test.sh stop         # Kill all shinzo-test processes
#   ./scripts/shinzo-test.sh clean        # Stop + wipe all data
#   ./scripts/shinzo-test.sh status       # Show running processes and data
#   ./scripts/shinzo-test.sh logs         # Tail both log files
#   ./scripts/shinzo-test.sh logs defra   # Tail defra log only
#   ./scripts/shinzo-test.sh logs indexer # Tail indexer log only
#   ./scripts/shinzo-test.sh query '{ Ethereum__Mainnet__Block { number hash } }'
#
# Rust FFI mode prerequisites:
#   1. cargo build --release --features fjall -p ffi
#   2. Copy libffi.a to app-sdk and weaken symbol:
#      cp target/release/libffi.a ../shinzo-app-sdk/pkg/rustffi/lib/
#      llvm-objcopy --weaken-symbol=_rust_eh_personality ../shinzo-app-sdk/pkg/rustffi/lib/libffi.a
#   3. cd ../shinzo-indexer-client && go build -o block_poster ./cmd/block_poster/
#
# Everything lives under one directory: /tmp/shinzo-test/
# Random ports are written to /tmp/shinzo-test/ports so other commands can find them.
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEFRA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFRA_BIN="${DEFRA_ROOT}/target/release/defra"
INDEXER_DIR="/Users/johnzampolin/go/src/github.com/shinzonetwork/shinzo-indexer-client"
SCHEMA_FILE="${INDEXER_DIR}/pkg/schema/schema_standard.graphql"

# Single well-known directory for everything
BASE_DIR="/tmp/shinzo-test"
DEFRA_DATA="${BASE_DIR}/defradb"
DEFRA_LOG="${BASE_DIR}/defra.log"
INDEXER_LOG="${BASE_DIR}/indexer.log"
PORTS_FILE="${BASE_DIR}/ports"
PIDS_FILE="${BASE_DIR}/pids"

# Default concurrency (can override: CONCURRENCY=1 ./scripts/shinzo-test.sh)
CONCURRENCY="${CONCURRENCY:-4}"
RECEIPT_WORKERS="${RECEIPT_WORKERS:-4}"
START_HEIGHT="${START_HEIGHT_OVERRIDE:-23700000}"
STORE="${STORE:-redb}"
# Set RUST_FFI=1 to use embedded Rust DefraDB via FFI (no separate defra process)
RUST_FFI="${RUST_FFI:-0}"
# Watchdog limits (override with env vars)
WATCHDOG_DISK_LIMIT_GB="${WATCHDOG_DISK_LIMIT_GB:-200}"
WATCHDOG_RSS_LIMIT_MB="${WATCHDOG_RSS_LIMIT_MB:-12000}"

# ---- helpers ----

die() { echo "ERROR: $*" >&2; exit 1; }

load_ports() {
  if [ -f "$PORTS_FILE" ]; then
    source "$PORTS_FILE"
  fi
}

load_pids() {
  DEFRA_PID=""
  INDEXER_PID=""
  WATCHDOG_PID=""
  if [ -f "$PIDS_FILE" ]; then
    source "$PIDS_FILE"
  fi
}

save_pids() {
  cat > "$PIDS_FILE" << EOF
DEFRA_PID=${DEFRA_PID:-}
INDEXER_PID=${INDEXER_PID:-}
EOF
}

is_alive() {
  [ -n "${1:-}" ] && kill -0 "$1" 2>/dev/null
}

random_port() {
  # Find a free port by trying to bind
  python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
"
}

kill_tracked() {
  load_pids
  local killed=0
  if is_alive "${DEFRA_PID:-}"; then
    echo "Stopping defradb.rs (PID ${DEFRA_PID})..."
    kill "$DEFRA_PID" 2>/dev/null || true
    sleep 1
    # Force kill if still alive
    if is_alive "$DEFRA_PID"; then
      kill -9 "$DEFRA_PID" 2>/dev/null || true
    fi
    killed=1
  fi
  if is_alive "${INDEXER_PID:-}"; then
    echo "Stopping indexer (PID ${INDEXER_PID})..."
    kill "$INDEXER_PID" 2>/dev/null || true
    sleep 1
    if is_alive "$INDEXER_PID"; then
      kill -9 "$INDEXER_PID" 2>/dev/null || true
    fi
    killed=1
  fi
  if is_alive "${WATCHDOG_PID:-}"; then
    echo "Stopping watchdog (PID ${WATCHDOG_PID})..."
    kill "$WATCHDOG_PID" 2>/dev/null || true
  fi
  # Also kill any orphaned processes matching our patterns
  pkill -f "defra start.*shinzo-test" 2>/dev/null || true
  pkill -f "block_poster.*shinzo-test" 2>/dev/null || true
  pkill -f "go-build.*main.*shinzo-test" 2>/dev/null || true
  pkill -f "watchdog.sh" 2>/dev/null || true
  rm -f "$PIDS_FILE"
  if [ $killed -eq 0 ]; then
    echo "No tracked processes to stop."
  fi
}

# ---- commands ----

cmd_stop() {
  kill_tracked
  echo "Done."
}

cmd_clean() {
  kill_tracked
  if [ -d "$BASE_DIR" ]; then
    echo "Wiping ${BASE_DIR}..."
    rm -rf "$BASE_DIR"
  fi
  echo "Clean."
}

cmd_status() {
  echo "=== Shinzo Test Status ==="
  echo ""

  if [ ! -d "$BASE_DIR" ]; then
    echo "No test directory (${BASE_DIR}). Run: ./scripts/shinzo-test.sh"
    return
  fi

  load_ports
  load_pids

  echo "Directory: ${BASE_DIR}"
  du -sh "$BASE_DIR" 2>/dev/null | awk '{print "Disk usage: " $1}'
  echo ""

  if [ -n "${HTTP_PORT:-}" ]; then
    echo "HTTP port: ${HTTP_PORT}"
    echo "P2P port:  ${P2P_PORT:-unknown}"
    echo "Store:     ${STORE:-unknown}"
  fi
  echo ""

  echo "Processes:"
  if is_alive "${DEFRA_PID:-}"; then
    echo "  defradb.rs: running (PID ${DEFRA_PID})"
  else
    echo "  defradb.rs: stopped"
  fi
  if is_alive "${INDEXER_PID:-}"; then
    echo "  indexer:    running (PID ${INDEXER_PID})"
  else
    echo "  indexer:    stopped"
  fi
  echo ""

  # Try to query block count
  if is_alive "${DEFRA_PID:-}" && [ -n "${HTTP_PORT:-}" ]; then
    local resp
    resp=$(curl -sf "http://127.0.0.1:${HTTP_PORT}/api/v0/graphql" \
      -X POST -H "Content-Type: application/json" \
      -d '{"query":"{ Ethereum__Mainnet__Block(order: {number: DESC}, limit: 1) { number } }"}' 2>/dev/null) || true
    if [ -n "$resp" ]; then
      echo "Latest block: $resp"
    fi
  fi
}

cmd_logs() {
  local target="${1:-all}"
  case "$target" in
    defra)
      [ -f "$DEFRA_LOG" ] && tail -f "$DEFRA_LOG" || echo "No defra log"
      ;;
    indexer)
      [ -f "$INDEXER_LOG" ] && tail -f "$INDEXER_LOG" || echo "No indexer log"
      ;;
    all|*)
      if [ -f "$DEFRA_LOG" ] && [ -f "$INDEXER_LOG" ]; then
        tail -f "$DEFRA_LOG" "$INDEXER_LOG"
      elif [ -f "$DEFRA_LOG" ]; then
        tail -f "$DEFRA_LOG"
      elif [ -f "$INDEXER_LOG" ]; then
        tail -f "$INDEXER_LOG"
      else
        echo "No logs found in ${BASE_DIR}"
      fi
      ;;
  esac
}

cmd_query() {
  local query="$1"
  load_ports
  if [ -z "${HTTP_PORT:-}" ]; then
    die "No ports file. Is defra running? Try: ./scripts/shinzo-test.sh status"
  fi
  curl -sf "http://127.0.0.1:${HTTP_PORT}/api/v0/graphql" \
    -X POST -H "Content-Type: application/json" \
    -d "{\"query\": $(echo "$query" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}" 2>/dev/null | python3 -m json.tool
}

start_watchdog() {
  local pid=$1
  local disk_limit_kb=$(( WATCHDOG_DISK_LIMIT_GB * 1024 * 1024 ))
  local rss_limit_kb=$(( WATCHDOG_RSS_LIMIT_MB * 1024 ))
  local watchdog_log="${BASE_DIR}/watchdog.log"

  cat > "${BASE_DIR}/watchdog.sh" << 'WATCHDOG'
#!/bin/bash
PID=$1; DISK_LIMIT_KB=$2; RSS_LIMIT_KB=$3; LOG=$4
while kill -0 "$PID" 2>/dev/null; do
  AVAIL_KB=$(df -k / | tail -1 | awk '{print $4}')
  RSS_KB=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
  [ -z "$RSS_KB" ] && break
  AVAIL_GB=$(( AVAIL_KB / 1024 / 1024 ))
  RSS_MB=$(( RSS_KB / 1024 ))
  if [ "$AVAIL_KB" -lt "$DISK_LIMIT_KB" ]; then
    echo "$(date): DISK LOW (${AVAIL_GB}GB free, limit ${DISK_LIMIT_KB}KB) - killing PID $PID" >> "$LOG"
    kill "$PID" 2>/dev/null
    exit 0
  fi
  if [ "$RSS_KB" -gt "$RSS_LIMIT_KB" ]; then
    echo "$(date): RSS HIGH (${RSS_MB}MB, limit $(( RSS_LIMIT_KB / 1024 ))MB) - killing PID $PID" >> "$LOG"
    kill "$PID" 2>/dev/null
    exit 0
  fi
  echo "$(date): OK disk=${AVAIL_GB}GB rss=${RSS_MB}MB" >> "$LOG"
  sleep 60
done
WATCHDOG
  chmod +x "${BASE_DIR}/watchdog.sh"
  bash "${BASE_DIR}/watchdog.sh" "$pid" "$disk_limit_kb" "$rss_limit_kb" "$watchdog_log" &
  local wd_pid=$!
  echo "WATCHDOG_PID=${wd_pid}" >> "$PIDS_FILE"
  echo "  Watchdog PID: ${wd_pid} (disk <${WATCHDOG_DISK_LIMIT_GB}GB, RSS <${WATCHDOG_RSS_LIMIT_MB}MB)"
}

cmd_start() {
  # Check prerequisites
  [ -d "$INDEXER_DIR" ] || die "indexer not found at ${INDEXER_DIR}"

  if [ "$RUST_FFI" = "1" ]; then
    cmd_start_rust_ffi
    return
  fi

  [ -f "$DEFRA_BIN" ] || die "defra binary not found. Run: cargo build --release"
  [ -f "$SCHEMA_FILE" ] || die "schema not found at ${SCHEMA_FILE}"

  # Stop any existing processes
  load_pids
  if is_alive "${DEFRA_PID:-}" || is_alive "${INDEXER_PID:-}"; then
    echo "Stopping existing processes..."
    kill_tracked
    sleep 1
  fi

  # Create base directory (preserve across runs unless 'clean' is called)
  mkdir -p "$BASE_DIR"

  # Pick random free ports
  HTTP_PORT=$(random_port)
  P2P_PORT=$(random_port)
  cat > "$PORTS_FILE" << EOF
HTTP_PORT=${HTTP_PORT}
P2P_PORT=${P2P_PORT}
STORE=${STORE}
EOF

  echo "=== Shinzo Integration Test ==="
  echo "  Base dir:     ${BASE_DIR}"
  echo "  HTTP:         http://127.0.0.1:${HTTP_PORT}"
  echo "  P2P:          /ip4/127.0.0.1/tcp/${P2P_PORT}"
  echo "  Store:        ${STORE}"
  echo "  Concurrency:  ${CONCURRENCY} blocks / ${RECEIPT_WORKERS} workers"
  echo "  Start height: ${START_HEIGHT}"
  echo ""

  # ---- Start defradb.rs ----
  echo "Starting defradb.rs..."
  DEFRA_KEYRING_SECRET=test-secret-shinzo \
    RUST_BACKTRACE=full \
    "$DEFRA_BIN" start \
    --rootdir "${DEFRA_DATA}" \
    --store "${STORE}" \
    --url "127.0.0.1:${HTTP_PORT}" \
    --p2paddr "/ip4/127.0.0.1/tcp/${P2P_PORT}" \
    --no-p2p true \
    > "$DEFRA_LOG" 2>&1 &
  DEFRA_PID=$!
  echo "  PID: ${DEFRA_PID}"

  # Wait for HTTP readiness
  echo -n "  Waiting for HTTP..."
  for i in $(seq 1 30); do
    sleep 0.5
    if curl -sf "http://127.0.0.1:${HTTP_PORT}/api/v0/schema" > /dev/null 2>&1; then
      echo " ready ($(echo "$i * 0.5" | bc)s)"
      break
    fi
    if ! is_alive "$DEFRA_PID"; then
      echo " CRASHED"
      echo ""
      echo "=== defra.log (last 30 lines) ==="
      tail -30 "$DEFRA_LOG"
      exit 1
    fi
    if [ "$i" -eq 30 ]; then
      echo " TIMEOUT (15s)"
      exit 1
    fi
  done

  # ---- Apply schema ----
  echo -n "  Applying schema..."
  local schema_resp
  schema_resp=$(curl -sf -X POST "http://127.0.0.1:${HTTP_PORT}/api/v0/schema" \
    -H "Content-Type: text/plain" \
    --data-binary "@${SCHEMA_FILE}" 2>&1) || {
    # Schema might already exist from a previous run (data not wiped)
    if echo "$schema_resp" | grep -q "already exists"; then
      echo " already exists (reusing)"
    else
      echo " FAILED"
      echo "  Response: ${schema_resp}"
      echo ""
      echo "  Hint: run './scripts/shinzo-test.sh clean' to wipe and retry"
      kill "$DEFRA_PID" 2>/dev/null || true
      exit 1
    fi
  }
  echo " OK (5 collections)"

  # Verify empty
  local block_resp
  block_resp=$(curl -sf "http://127.0.0.1:${HTTP_PORT}/api/v0/graphql" \
    -X POST -H "Content-Type: application/json" \
    -d '{"query":"{ Ethereum__Mainnet__Block { number } }"}' 2>/dev/null) || true
  if echo "$block_resp" | python3 -c "import sys,json; d=json.load(sys.stdin); exit(0 if len(d.get('data',{}).get('Ethereum__Mainnet__Block',[])) == 0 else 1)" 2>/dev/null; then
    echo "  Database: empty (fresh start)"
  else
    echo "  Database: has existing data (resuming)"
  fi

  # ---- Generate indexer config ----
  local indexer_config="${BASE_DIR}/indexer-config.yaml"
  cat > "$indexer_config" << YAML
defradb:
  url: "http://127.0.0.1:${HTTP_PORT}"
  keyring_secret: ""
  embedded: false
  p2p:
    enabled: false
    bootstrap_peers: []
    listen_addr: "/ip4/0.0.0.0/tcp/0"
    max_retries: 5
    retry_base_delay_ms: 1000
    reconnect_interval_ms: 60000
    enable_auto_reconnect: false
  store:
    path: "${BASE_DIR}/defra-store"
    block_cache_mb: 1024
    memtable_mb: 128
    index_cache_mb: 512
    num_compactors: 4

geth:
  node_url: "\${GETH_RPC_URL}"
  ws_url: "\${GETH_WS_URL}"
  api_key: "\${GETH_API_KEY}"

indexer:
  start_height: ${START_HEIGHT}
  concurrent_blocks: ${CONCURRENCY}
  receipt_workers: ${RECEIPT_WORKERS}
  max_docs_per_txn: 500
  blocks_per_minute: 0
  health_server_port: 0
  pprof_port: 0
  open_browser_on_start: false

logger:
  development: false
  level: "info"
YAML

  # ---- Source .env for geth credentials ----
  if [ -f "${INDEXER_DIR}/.env" ]; then
    set -a
    source "${INDEXER_DIR}/.env"
    set +a
  else
    die "No .env file at ${INDEXER_DIR}/.env (need GETH_RPC_URL, GETH_WS_URL, GETH_API_KEY)"
  fi

  # ---- Start indexer ----
  echo ""
  echo "Starting indexer..."
  cd "$INDEXER_DIR"
  HTTPS_PROXY="${HTTPS_PROXY:-${ALL_PROXY:-}}" \
    HTTP_PROXY="${HTTP_PROXY:-${ALL_PROXY:-}}" \
    ./block_poster -config "$indexer_config" > "$INDEXER_LOG" 2>&1 &
  INDEXER_PID=$!
  echo "  PID: ${INDEXER_PID}"
  cd "$DEFRA_ROOT"

  # Save PIDs
  save_pids

  echo ""
  echo "=== Running ==="
  echo "  defra log:   tail -f ${DEFRA_LOG}"
  echo "  indexer log:  tail -f ${INDEXER_LOG}"
  echo "  both logs:    ./scripts/shinzo-test.sh logs"
  echo "  query:        ./scripts/shinzo-test.sh query '{ Ethereum__Mainnet__Block(limit: 5) { number hash } }'"
  echo "  status:       ./scripts/shinzo-test.sh status"
  echo "  stop:         ./scripts/shinzo-test.sh stop"
  echo "  wipe+restart: ./scripts/shinzo-test.sh clean && ./scripts/shinzo-test.sh"
  echo ""

  # Stream indexer output to terminal
  echo "=== Indexer Output (Ctrl-C to detach, processes keep running) ==="
  tail -f "$INDEXER_LOG" || true
}

# Start in Rust FFI embedded mode: no separate defra process, the indexer embeds
# Rust DefraDB directly via FFI with the fjall storage backend.
cmd_start_rust_ffi() {
  # Stop any existing processes
  load_pids
  if is_alive "${DEFRA_PID:-}" || is_alive "${INDEXER_PID:-}"; then
    echo "Stopping existing processes..."
    kill_tracked
    sleep 1
  fi

  mkdir -p "$BASE_DIR"

  # Default to fjall for FFI mode, but allow override via STORE env var
  FFI_STORE="${STORE:-fjall}"
  cat > "$PORTS_FILE" << EOF
RUST_FFI=1
STORE=${FFI_STORE}
EOF

  echo "=== Shinzo Integration Test (Rust FFI Embedded) ==="
  echo "  Base dir:     ${BASE_DIR}"
  echo "  Mode:         Rust FFI embedded (${FFI_STORE} backend)"
  echo "  Concurrency:  ${CONCURRENCY} blocks / ${RECEIPT_WORKERS} workers"
  echo "  Start height: ${START_HEIGHT}"
  echo ""

  # ---- Generate indexer config ----
  local indexer_config="${BASE_DIR}/indexer-config.yaml"
  cat > "$indexer_config" << YAML
defradb:
  url: ""
  keyring_secret: ""
  embedded: true
  use_rust_ffi: true
  p2p:
    enabled: false
    bootstrap_peers: []
    listen_addr: "/ip4/0.0.0.0/tcp/0"
    max_retries: 5
    retry_base_delay_ms: 1000
    reconnect_interval_ms: 60000
    enable_auto_reconnect: false
  store:
    path: "${BASE_DIR}/rust-ffi-data"

geth:
  node_url: "\${GETH_RPC_URL}"
  ws_url: "\${GETH_WS_URL}"
  api_key: "\${GETH_API_KEY}"

indexer:
  start_height: ${START_HEIGHT}
  concurrent_blocks: ${CONCURRENCY}
  receipt_workers: ${RECEIPT_WORKERS}
  max_docs_per_txn: 500
  blocks_per_minute: 0
  health_server_port: 0
  pprof_port: 0
  open_browser_on_start: false

logger:
  development: false
YAML

  # ---- Source .env for geth credentials ----
  if [ -f "${INDEXER_DIR}/.env" ]; then
    set -a
    source "${INDEXER_DIR}/.env"
    set +a
  else
    die "No .env file at ${INDEXER_DIR}/.env (need GETH_RPC_URL, GETH_WS_URL, GETH_API_KEY)"
  fi

  # ---- Log RocksDB tuning if applicable ----
  if [ "${FFI_STORE}" = "rocksdb" ]; then
    echo "  RocksDB tuning (env overrides):"
    for var in ROCKS_BLOCK_CACHE_MB ROCKS_WRITE_BUFFER_MB ROCKS_MAX_WRITE_BUFFERS \
               ROCKS_COMPACTIONS ROCKS_FLUSHES ROCKS_L0_SLOWDOWN ROCKS_L0_STOP \
               ROCKS_TARGET_FILE_MB ROCKS_LEVEL_BASE_MB ROCKS_BLOCK_SIZE_KB \
               ROCKS_COMPRESSION ROCKS_COMPACTION_STYLE ROCKS_BLOB_FILES ROCKS_MIN_BLOB_SIZE; do
      val="${!var:-}"
      if [ -n "$val" ]; then
        echo "    ${var}=${val}"
      fi
    done
    echo "    (unset vars use defaults: cache=512MB, wbuf=64MB, wbufs=4, compact=4, flush=2)"
    echo ""
  fi

  # ---- Start indexer (no separate defra needed) ----
  echo "Starting indexer with embedded Rust DefraDB..."
  cd "$INDEXER_DIR"
  STORE="${FFI_STORE}" \
    ROCKS_BLOCK_CACHE_MB="${ROCKS_BLOCK_CACHE_MB:-}" \
    ROCKS_WRITE_BUFFER_MB="${ROCKS_WRITE_BUFFER_MB:-}" \
    ROCKS_MAX_WRITE_BUFFERS="${ROCKS_MAX_WRITE_BUFFERS:-}" \
    ROCKS_COMPACTIONS="${ROCKS_COMPACTIONS:-}" \
    ROCKS_FLUSHES="${ROCKS_FLUSHES:-}" \
    ROCKS_L0_SLOWDOWN="${ROCKS_L0_SLOWDOWN:-}" \
    ROCKS_L0_STOP="${ROCKS_L0_STOP:-}" \
    ROCKS_TARGET_FILE_MB="${ROCKS_TARGET_FILE_MB:-}" \
    ROCKS_LEVEL_BASE_MB="${ROCKS_LEVEL_BASE_MB:-}" \
    ROCKS_BLOCK_SIZE_KB="${ROCKS_BLOCK_SIZE_KB:-}" \
    ROCKS_COMPRESSION="${ROCKS_COMPRESSION:-}" \
    ROCKS_COMPACTION_STYLE="${ROCKS_COMPACTION_STYLE:-}" \
    ROCKS_BLOB_FILES="${ROCKS_BLOB_FILES:-}" \
    ROCKS_MIN_BLOB_SIZE="${ROCKS_MIN_BLOB_SIZE:-}" \
    HTTPS_PROXY="${HTTPS_PROXY:-${ALL_PROXY:-}}" \
    HTTP_PROXY="${HTTP_PROXY:-${ALL_PROXY:-}}" \
    ./block_poster -config "$indexer_config" > "$INDEXER_LOG" 2>&1 &
  INDEXER_PID=$!
  echo "  PID: ${INDEXER_PID}"
  cd "$DEFRA_ROOT"

  # Save PIDs (no DEFRA_PID in FFI mode)
  DEFRA_PID=""
  save_pids

  # Start watchdog (kills indexer if disk or RSS exceed limits)
  start_watchdog "$INDEXER_PID"

  echo ""
  echo "=== Running ==="
  echo "  indexer log:  tail -f ${INDEXER_LOG}"
  echo "  watchdog log: tail -f ${BASE_DIR}/watchdog.log"
  echo "  status:       ./scripts/shinzo-test.sh status"
  echo "  stop:         ./scripts/shinzo-test.sh stop"
  echo "  wipe+restart: ./scripts/shinzo-test.sh clean && RUST_FFI=1 ./scripts/shinzo-test.sh"
  echo ""

  # Stream indexer output to terminal
  echo "=== Indexer Output (Ctrl-C to detach, processes keep running) ==="
  tail -f "$INDEXER_LOG" || true
}

cmd_monitor() {
  load_ports
  load_pids
  if ! is_alive "${DEFRA_PID:-}" && ! is_alive "${INDEXER_PID:-}"; then
    echo "Nothing running. Start first: ./scripts/shinzo-test.sh"
    return
  fi

  echo "=== Monitoring (Ctrl-C to stop) ==="
  echo ""
  while true; do
    local ts
    ts=$(date +"%H:%M:%S")
    local defra_mem="" indexer_mem="" defra_cpu="" indexer_cpu=""
    local disk=""

    if is_alive "${DEFRA_PID:-}"; then
      defra_mem=$(ps -o rss= -p "$DEFRA_PID" 2>/dev/null | awk '{printf "%.0fMB", $1/1024}')
      defra_cpu=$(ps -o %cpu= -p "$DEFRA_PID" 2>/dev/null | awk '{printf "%.0f%%", $1}')
    else
      defra_mem="DEAD"
      defra_cpu="-"
    fi

    if is_alive "${INDEXER_PID:-}"; then
      indexer_mem=$(ps -o rss= -p "$INDEXER_PID" 2>/dev/null | awk '{printf "%.0fMB", $1/1024}')
      indexer_cpu=$(ps -o %cpu= -p "$INDEXER_PID" 2>/dev/null | awk '{printf "%.0f%%", $1}')
    else
      indexer_mem="DEAD"
      indexer_cpu="-"
    fi

    disk=$(du -sh "$BASE_DIR" 2>/dev/null | awk '{print $1}')

    # Get latest block from indexer log
    local latest_block=""
    latest_block=$(grep -o 'height.*[0-9]\+' "$INDEXER_LOG" 2>/dev/null | tail -1 | grep -o '[0-9]\+$' || echo "?")

    # Count errors in last 10 seconds of indexer log
    local recent_errors=""
    recent_errors=$(tail -100 "$INDEXER_LOG" 2>/dev/null | grep -c "ERROR" || echo "0")

    printf "[%s] defra: %s/%s  indexer: %s/%s  disk: %s  block: %s  errs(recent): %s\n" \
      "$ts" "$defra_mem" "$defra_cpu" "$indexer_mem" "$indexer_cpu" "$disk" "$latest_block" "$recent_errors"

    # Check for defra crash
    if [ "$defra_mem" = "DEAD" ]; then
      echo ""
      echo "!!! DEFRA CRASHED - last 10 lines of defra.log:"
      tail -10 "$DEFRA_LOG" 2>/dev/null
      echo ""
      echo "Check crash report: ls -lt ~/Library/Logs/DiagnosticReports/ | head -5"
      break
    fi

    sleep 5
  done
}

# ---- main ----

case "${1:-start}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  clean)   cmd_clean ;;
  status)  cmd_status ;;
  monitor) cmd_monitor ;;
  logs)    cmd_logs "${2:-all}" ;;
  query)   cmd_query "${2:?Usage: shinzo-test.sh query 'GRAPHQL'}" ;;
  *)       echo "Usage: $0 {start|stop|clean|status|monitor|logs [defra|indexer]|query GRAPHQL}"; exit 1 ;;
esac
