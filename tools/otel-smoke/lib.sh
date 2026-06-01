#!/usr/bin/env bash
# Shared scaffolding for the OTel smoke scripts (run.sh, dedup.sh).
# Source this; it does not run anything on its own.
#
# Exposes:
#   SCRIPT_DIR / REPO_ROOT / OUTPUT_DIR  — resolved paths
#   DEFRA_PID                            — set by start_defra, cleared by stop_defra
#   build_defra                          — cargo build -p cli --features otel
#   start_defra <port> <stdout> <stderr> — launch `defra start` (memory store, no keyring)
#   wait_for_http <port> <stderr>        — poll until the HTTP API answers, or fail
#   stop_defra                           — SIGTERM + wait, clears DEFRA_PID
#
# Callers pin the OTLP endpoint to 127.0.0.1 (not `localhost`) so reqwest's
# resolver and any docker port binding agree on IPv4.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
OUTPUT_DIR="$SCRIPT_DIR/output"

DEFRA_PID=""

build_defra() {
  echo "=== building defra (cli --features otel) ==="
  (cd "$REPO_ROOT" && cargo build -p cli --features otel)
}

# start_defra <port> <stdout_log> <stderr_log>
start_defra() {
  local port="$1" stdout_log="$2" stderr_log="$3"
  local rootdir
  rootdir="$(mktemp -d)"
  echo "=== starting defra on 127.0.0.1:$port ==="
  "$REPO_ROOT/target/debug/defra" start \
    --rootdir "$rootdir" \
    --no-keyring \
    --store memory \
    --url "127.0.0.1:$port" \
    >"$stdout_log" 2>"$stderr_log" &
  DEFRA_PID=$!
}

# wait_for_http <port> <stderr_log>
# Probes a real 200 route (`/api/v0/schema`); `/` returns 404, so `curl -sf`
# against it never succeeds.
wait_for_http() {
  local port="$1" stderr_log="$2"
  echo "=== waiting for defra HTTP :$port ==="
  for _ in $(seq 1 30); do
    if curl -sf -o /dev/null "http://127.0.0.1:$port/api/v0/schema"; then
      return 0
    fi
    if ! kill -0 "$DEFRA_PID" 2>/dev/null; then
      echo "FAIL: defra exited before serving; stderr below"
      tail -100 "$stderr_log"
      exit 1
    fi
    sleep 1
  done
  echo "FAIL: defra HTTP :$port did not come up"
  exit 1
}

stop_defra() {
  if [ -n "$DEFRA_PID" ]; then
    kill -TERM "$DEFRA_PID" 2>/dev/null || true
    wait "$DEFRA_PID" 2>/dev/null || true
    DEFRA_PID=""
  fi
}
