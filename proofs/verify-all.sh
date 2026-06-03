#!/usr/bin/env bash
# Single entry point: verify every formal-methods artifact (TLA+ models + Lean proofs).
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "== TLA+ models (proofs/tla/run-all.sh) =="
( cd "$DIR/tla" && ./run-all.sh ); tla=$?
echo; echo "== Lean proofs (proofs/lean: lake build) =="
( cd "$DIR/lean" && lake build && echo "lake build: OK" ); lean=$?
echo "----"
if [ "$tla" -eq 0 ] && [ "$lean" -eq 0 ]; then echo "ALL GREEN — TLA+ regression matched and Lean built clean."; exit 0
else echo "FAILURE (tla exit=$tla, lean exit=$lean)"; exit 1; fi
