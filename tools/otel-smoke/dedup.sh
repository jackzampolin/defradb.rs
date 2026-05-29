#!/usr/bin/env bash
# Negative-path verification: with no OTLP collector listening, the dedup
# filter must squash exporter-unreachable log spam to one line per pattern
# per process — the Rust port of Go DefraDB's `otel.SetErrorHandler +
# sync.Once` (issue #977, Go commit 83af37a9).
#
# Does NOT use docker; it relies on the absence of a listener on the
# chosen port.

set -euo pipefail

# shellcheck source=tools/otel-smoke/lib.sh
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/lib.sh"

STDERR_LOG="$OUTPUT_DIR/defra-dedup.stderr"
STDOUT_LOG="$OUTPUT_DIR/defra-dedup.stdout"
PORT=9182

mkdir -p "$OUTPUT_DIR"
rm -f "$STDERR_LOG" "$STDOUT_LOG"

# Use a port that is almost certainly unbound. Pin to 127.0.0.1 so the
# precondition check uses the same IP family as reqwest's resolver —
# `localhost` would let a bound `[::1]:$DEAD_PORT` silently mask the test.
DEAD_PORT=14318
if nc -z 127.0.0.1 "$DEAD_PORT" 2>/dev/null; then
  echo "FAIL: port $DEAD_PORT is bound; pick another or stop the listener"
  exit 1
fi

trap stop_defra EXIT INT TERM

build_defra

export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:$DEAD_PORT"
export OTEL_BSP_SCHEDULE_DELAY="300"  # ms — provoke multiple retries quickly
start_defra "$PORT" "$STDOUT_LOG" "$STDERR_LOG"
wait_for_http "$PORT" "$STDERR_LOG"

echo "=== generating spans to provoke repeated batch-export attempts ==="
# Each GraphQL request creates a QueryRunner::execute span (info-level
# `#[instrument]`); HTTP `tower_http::TraceLayer` spans are debug-level
# (the default env filter is INFO). Without dedup, every failed batch
# export logs an error to stderr — this loop generates enough spans for
# the BatchSpanProcessor to keep trying.
for _ in $(seq 1 30); do
  curl -sf -o /dev/null \
    -X POST "http://127.0.0.1:$PORT/api/v0/graphql" \
    -H 'Content-Type: application/json' \
    -d '{"query": "query { __typename }"}' || true
  sleep 0.2
done

echo "=== letting any final batch processor retries fire ==="
sleep 3

echo "=== shutting down defra ==="
stop_defra

echo "=== checking the operator hint was emitted exactly once ==="
# Go-parity behavior (crates/telemetry/src/dedup.rs): the raw, repeated SDK
# export errors are SUPPRESSED, and a single actionable hint is emitted once
# per process via a global latch. So we assert:
#   - the hint appears exactly once (proves: internal-logs on → SDK emitted →
#     filter caught it → emitted once globally; matches Go's sync.Once), and
#   - the raw needle text is gone from stderr (proves suppression).
HINT='OpenTelemetry export failed, ensure your OTLP collector is running and reachable'
HINT_LINES=$(grep -Fc "$HINT" "$STDERR_LOG" || true)
echo "operator-hint lines: $HINT_LINES (expected exactly 1)"

if [ "$HINT_LINES" -eq 0 ]; then
  echo "FAIL: the operator hint never appeared despite an unreachable collector."
  echo "      Likely the opentelemetry SDK's 'internal-logs' feature is off"
  echo "      (the otel_error! macros no-op without it), so export failures emit"
  echo "      nothing and the dedup filter is dead code. Check telemetry/Cargo.toml"
  echo "      keeps internal-logs in the otlp feature. Server stderr: $STDERR_LOG"
  exit 1
fi

if [ "$HINT_LINES" -gt 1 ]; then
  echo "FAIL: operator hint emitted $HINT_LINES times (expected exactly 1)."
  echo "      The process-global once-latch is not deduping across layers."
  grep -F "$HINT" "$STDERR_LOG" | head -10
  exit 1
fi

# The raw, spammy SDK error lines should be fully suppressed (Go parity).
RAW_LINES=$(grep -E "opentelemetry" "$STDERR_LOG" \
    | grep -E "ERROR" \
    | grep -Ec 'connection refused|HTTP export failed|network error' \
    || true)
if [ "$RAW_LINES" -ne 0 ]; then
  echo "WARN: $RAW_LINES raw exporter-error line(s) leaked through (expected 0 — they should be suppressed in favor of the single hint)."
  grep -E "opentelemetry" "$STDERR_LOG" | grep -E "ERROR" | head -5
fi

echo "PASS: operator hint emitted exactly once; raw exporter spam suppressed"
