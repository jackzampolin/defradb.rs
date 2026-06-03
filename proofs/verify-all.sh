#!/usr/bin/env bash
# Single entry point: verify every formal-methods artifact and its binding to code.
#   1. TLA+ models   — red/green oracle (proofs/tla/run-all.sh)
#   2. Lean proofs   — builds clean, zero `sorry` (proofs/lean: lake build)
#   3. Conformance   — models <-> code:
#        - Lean axis (fast, no binary): contract vocab asserted against Rust types
#        - TLA axis (behavioral): drives the real release binary; skipped if absent
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/.." && pwd)"

echo "== TLA+ models (proofs/tla/run-all.sh) =="
( cd "$DIR/tla" && ./run-all.sh ); tla=$?

echo; echo "== Lean proofs (proofs/lean: lake build) =="
( cd "$DIR/lean" && lake build && echo "lake build: OK" ); lean=$?

echo; echo "== Conformance: Lean axis + registry (cargo test, no binary) =="
( cd "$ROOT" && cargo test -p conformance --lib --test lean_conformance ); conf=$?

echo; echo "== Conformance: TLA axis behavioral (release binary) =="
if [ -x "$ROOT/target/release/defra" ] || [ -n "${DEFRA_CONFORMANCE_BINARY:-}" ]; then
  ( cd "$ROOT" && cargo test -p conformance --test tla_conformance ); behav=$?
else
  echo "SKIP — no release binary. Build it: cargo build --release -p cli"
  echo "       (or set DEFRA_CONFORMANCE_BINARY to a shipped artifact)"
  behav=0
fi

echo "----"
if [ "$tla" -eq 0 ] && [ "$lean" -eq 0 ] && [ "$conf" -eq 0 ] && [ "$behav" -eq 0 ]; then
  echo "ALL GREEN — TLA+ matched, Lean built clean, conformance bound."
  exit 0
fi
echo "FAILURE (tla=$tla lean=$lean conformance=$conf behavioral=$behav)"
exit 1
