#!/usr/bin/env bash
# End-to-end OTel exporter smoke test against a real OTLP collector + Jaeger.
#
# 1. Stands up otel-collector-contrib + Jaeger in docker compose.
# 2. Builds and starts `defra start --features otel`, pointed at the collector.
# 3. Makes a handful of HTTP / GraphQL requests so spans are emitted.
# 4. SIGTERMs defra so the provider shutdown flushes the batch.
# 5. Reads the collector's file exporter output and asserts that:
#    - service.name=DefraDB resource attribute is present
#    - At least one span batch landed
#
# Pass `--keep` to leave the stack up after the run (useful for inspecting
# spans in the Jaeger UI at http://localhost:16686).

set -euo pipefail

KEEP=0
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
OUTPUT_DIR="$SCRIPT_DIR/output"
SPANS_FILE="$OUTPUT_DIR/spans.jsonl"
DEFRA_STDERR="$OUTPUT_DIR/defra.stderr"
DEFRA_STDOUT="$OUTPUT_DIR/defra.stdout"

DEFRA_PID=""

cleanup() {
  local rc=$?
  if [ -n "$DEFRA_PID" ]; then
    kill -TERM "$DEFRA_PID" 2>/dev/null || true
    wait "$DEFRA_PID" 2>/dev/null || true
  fi
  if [ "$KEEP" -eq 0 ]; then
    (cd "$SCRIPT_DIR" && docker compose down -v >/dev/null 2>&1) || true
  else
    echo "--- stack left running (--keep). Jaeger UI: http://localhost:16686 ---"
    echo "--- tear down with:  (cd $SCRIPT_DIR && docker compose down -v) ---"
  fi
  exit $rc
}
trap cleanup EXIT INT TERM

step() { printf '\n=== %s ===\n' "$1"; }

step "preparing output dir"
mkdir -p "$OUTPUT_DIR"
rm -f "$SPANS_FILE" "$DEFRA_STDERR" "$DEFRA_STDOUT"
# otelcol runs as non-root inside the container and needs write access.
chmod 777 "$OUTPUT_DIR"

step "starting otelcol + jaeger"
(cd "$SCRIPT_DIR" && docker compose up -d)

step "waiting for otelcol :4318"
for _ in $(seq 1 30); do
  if nc -z 127.0.0.1 4318 2>/dev/null; then
    break
  fi
  sleep 1
done
nc -z 127.0.0.1 4318 || { echo "FAIL: otelcol did not come up"; exit 1; }

step "building defra (cli --features otel)"
(cd "$REPO_ROOT" && cargo build -p cli --features otel)

step "starting defra against http://127.0.0.1:4318"
DEFRA_ROOT="$(mktemp -d)"
# 127.0.0.1 (not `localhost`) so reqwest's resolver and the docker port
# binding agree on IPv4 — `localhost` would otherwise prefer ::1 on some
# Linux setups and miss the docker container that only listens on v4.
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4318"
export OTEL_BSP_SCHEDULE_DELAY="500"  # ms — flush quickly so the test isn't slow
# Memory store keeps the test hermetic.
"$REPO_ROOT/target/debug/defra" start \
  --rootdir "$DEFRA_ROOT" \
  --no-keyring \
  --store memory \
  --url 127.0.0.1:9181 \
  >"$DEFRA_STDOUT" 2>"$DEFRA_STDERR" &
DEFRA_PID=$!

step "waiting for defra HTTP :9181"
for _ in $(seq 1 30); do
  if curl -sf -o /dev/null http://127.0.0.1:9181/ ; then
    break
  fi
  if ! kill -0 "$DEFRA_PID" 2>/dev/null; then
    echo "FAIL: defra exited; stderr below"
    cat "$DEFRA_STDERR"
    exit 1
  fi
  sleep 1
done

step "exercising endpoints to emit spans"
# Any HTTP request produces a tower_http TraceLayer span; GraphQL adds the
# QueryRunner::execute span (already #[instrument]'d).
curl -s -o /dev/null http://127.0.0.1:9181/ || true
curl -s -o /dev/null http://127.0.0.1:9181/api/v0/schema || true
curl -s -o /dev/null \
  -X POST http://127.0.0.1:9181/api/v0/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query": "query { __typename }"}' || true

step "waiting for batch flush"
sleep 2

step "shutting down defra (flushes provider)"
kill -TERM "$DEFRA_PID"
wait "$DEFRA_PID" 2>/dev/null || true
DEFRA_PID=""

step "waiting for collector to flush file exporter"
sleep 2

step "asserting spans landed"
if [ ! -s "$SPANS_FILE" ]; then
  echo "FAIL: $SPANS_FILE is empty or missing"
  echo "--- defra stderr ---"
  tail -100 "$DEFRA_STDERR"
  exit 1
fi

BATCH_COUNT=$(wc -l < "$SPANS_FILE" | tr -d '[:space:]')
echo "spans.jsonl: $BATCH_COUNT batch(es)"

if ! grep -q '"DefraDB"' "$SPANS_FILE"; then
  echo "FAIL: service.name=DefraDB not found in exported batches"
  echo "--- first batch (head) ---"
  head -c 2000 "$SPANS_FILE"
  exit 1
fi
echo "  service.name=DefraDB ............................. ok"

for attr in "service.version" "os.type" "host.arch" "process.pid" "process.executable.name"; do
  if grep -q "\"$attr\"" "$SPANS_FILE"; then
    echo "  $attr ............................. ok"
  else
    echo "  $attr ............................. MISSING"
  fi
done

# Look for the key span names we added or expect.
for span in '"request"' '"query.execute_request"'; do
  if grep -q "$span" "$SPANS_FILE"; then
    echo "  span $span ........... ok"
  else
    echo "  span $span ........... not found (may need different endpoint)"
  fi
done

step "PASS"
echo "Jaeger UI:    http://localhost:16686  (search for service 'DefraDB')"
echo "Spans file:   $SPANS_FILE"
