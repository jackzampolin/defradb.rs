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
# Each known needle (connection refused / HTTP export failed / network error)
# has its own per-process latch — so the upper bound is one log line per
# distinct pattern. In practice the SDK's unreachable-collector message
# ("HTTP export failed: network error") matches two needles in one line and
# claims both latches at once, so the realistic ceiling is 2, not 3. A
# regression to first-match-wins would leak a 3rd line and trip this.
# See crates/telemetry/src/dedup.rs.
NEEDLE_MAX=2
PATTERN='connection refused|HTTP export failed|network error'
UNREACHABLE_LINES=$(grep -E "ERROR" "$STDERR_LOG" \
    | grep -E "opentelemetry" \
    | grep -Ec "$PATTERN" \
    || true)
echo "exporter-unreachable lines: $UNREACHABLE_LINES (per-needle ceiling: $NEEDLE_MAX)"

if [ "$UNREACHABLE_LINES" -gt "$NEEDLE_MAX" ]; then
  echo "FAIL: dedup let $UNREACHABLE_LINES exporter-unreachable lines through (expected at most $NEEDLE_MAX, one per needle)"
  echo "--- offending lines ---"
  grep -E "ERROR" "$STDERR_LOG" | grep -E "opentelemetry" | grep -E "$PATTERN" | head -20
  exit 1
fi

# Zero lines is a FAIL, not informational: with a guaranteed-unreachable
# collector the SDK must emit at least one export-failure event. Zero means
# either the dedup is over-suppressing OR — the regression this guards — the
# SDK's `internal-logs` feature got disabled and the exporter went silent
# (which would also make the dedup filter dead code). Either way it's a real
# defect, so fail loudly.
if [ "$UNREACHABLE_LINES" -eq 0 ]; then
  echo "FAIL: zero exporter-unreachable lines despite an unreachable collector."
  echo "      Likely the opentelemetry SDK's 'internal-logs' feature is off"
  echo "      (the otel_error! macros no-op without it), so export failures emit"
  echo "      nothing and the dedup filter is dead code. Check telemetry/Cargo.toml"
  echo "      keeps internal-logs in the otlp feature. Server stderr: $STDERR_LOG"
  exit 1
fi

echo "PASS: $UNREACHABLE_LINES exporter-unreachable log line(s), within [1, $NEEDLE_MAX]"
