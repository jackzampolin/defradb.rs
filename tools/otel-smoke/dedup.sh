#!/usr/bin/env bash
# Negative-path verification: with no OTLP collector listening, the dedup
# filter must squash "connection refused" log spam to a single occurrence
# per process — the Rust port of Go DefraDB's `otel.SetErrorHandler +
# sync.Once` (issue #977, Go commit 83af37a9).
#
# Does NOT use docker; it relies on the absence of a listener on the
# chosen port.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
STDERR_LOG="$SCRIPT_DIR/output/defra-dedup.stderr"
STDOUT_LOG="$SCRIPT_DIR/output/defra-dedup.stdout"

mkdir -p "$SCRIPT_DIR/output"
rm -f "$STDERR_LOG" "$STDOUT_LOG"

# Use a port that is almost certainly unbound.
DEAD_PORT=14318
# Pin to 127.0.0.1 so the precondition check uses the same IP family as
# reqwest's resolver — `localhost` would let a bound `[::1]:$DEAD_PORT`
# silently mask the test.
if nc -z 127.0.0.1 "$DEAD_PORT" 2>/dev/null; then
  echo "FAIL: port $DEAD_PORT is bound; pick another or stop the listener"
  exit 1
fi

DEFRA_PID=""
cleanup() {
  if [ -n "$DEFRA_PID" ]; then
    kill -TERM "$DEFRA_PID" 2>/dev/null || true
    wait "$DEFRA_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "=== building defra (cli --features otel) ==="
(cd "$REPO_ROOT" && cargo build -p cli --features otel)

echo "=== starting defra against unreachable collector ==="
DEFRA_ROOT="$(mktemp -d)"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:$DEAD_PORT"
export OTEL_BSP_SCHEDULE_DELAY="300"  # ms — provoke multiple retries quickly
"$REPO_ROOT/target/debug/defra" start \
  --rootdir "$DEFRA_ROOT" \
  --no-keyring \
  --store memory \
  --url 127.0.0.1:9182 \
  >"$STDOUT_LOG" 2>"$STDERR_LOG" &
DEFRA_PID=$!

echo "=== waiting for defra HTTP :9182 ==="
for _ in $(seq 1 30); do
  if curl -sf -o /dev/null http://127.0.0.1:9182/ ; then
    break
  fi
  if ! kill -0 "$DEFRA_PID" 2>/dev/null; then
    echo "FAIL: defra exited; stderr below"
    tail -50 "$STDERR_LOG"
    exit 1
  fi
  sleep 1
done

echo "=== generating spans to provoke repeated batch-export attempts ==="
# Each GraphQL request creates a QueryRunner::execute span (info-level
# `#[instrument]`); HTTP `tower_http::TraceLayer` spans are debug-level
# (the default env filter is INFO). Without dedup, every failed batch
# export logs an error to stderr — this loop generates enough spans for
# the BatchSpanProcessor to keep trying.
for _ in $(seq 1 30); do
  curl -sf -o /dev/null \
    -X POST http://127.0.0.1:9182/api/v0/graphql \
    -H 'Content-Type: application/json' \
    -d '{"query": "query { __typename }"}' || true
  sleep 0.2
done

echo "=== letting any final batch processor retries fire ==="
sleep 3

echo "=== shutting down defra ==="
kill -TERM "$DEFRA_PID"
wait "$DEFRA_PID" 2>/dev/null || true
DEFRA_PID=""

echo "=== counting exporter-unreachable log lines in stderr ==="
# These are the messages the dedup filter is responsible for squashing.
# Counted as ERROR-level events from any `opentelemetry*` target, matching
# the same needles the filter does (see crates/telemetry/src/dedup.rs).
PATTERN='connection refused|HTTP export failed|network error'
UNREACHABLE_LINES=$(grep -E "ERROR" "$STDERR_LOG" \
    | grep -E "opentelemetry" \
    | grep -Ec "$PATTERN" \
    || true)
echo "exporter-unreachable lines: $UNREACHABLE_LINES"

if [ "$UNREACHABLE_LINES" -gt 1 ]; then
  echo "FAIL: dedup let $UNREACHABLE_LINES exporter-unreachable lines through (expected at most 1)"
  echo "--- offending lines ---"
  grep -E "ERROR" "$STDERR_LOG" | grep -E "opentelemetry" | grep -E "$PATTERN" | head -20
  exit 1
fi

if [ "$UNREACHABLE_LINES" -eq 0 ]; then
  echo "WARN: zero exporter-unreachable lines logged."
  echo "      Either the dedup is doing its job AND the test didn't provoke any pre-dedup logs"
  echo "      (e.g. SDK self-throttled), or the exporter was reachable. Inspect $STDERR_LOG"
  echo "      to confirm."
fi

echo "PASS: at most 1 exporter-unreachable log line (got $UNREACHABLE_LINES)"
